//! Where a state stream's bytes are, and positional reads out of it.
//!
//! A file-backed stream is opened and checked against the length its handle records
//! ([`crate::state_file::open`]) when its operator is opened, which also reads its header; the
//! handle is then [closed](Source::close), so opening an operator of any parallelism holds one
//! file at a time. Key-group reads then go through positional reads (`pread` on Unix,
//! `seek_read` on Windows), so concurrent readers share one handle: the export keeps its own
//! handle per stream ([`Source::read`]), a `StateReader` takes handles from a [`FilePool`] of at
//! most a few dozen open files, least recently used closed first ([`Source::read_pooled`]), so a
//! handle is reopened only after it has been evicted, never per key-group. An inline stream (`ByteStreamStateHandle`) is
//! never copied: its bytes stay in the parsed `_metadata` and are borrowed from there.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::parser::state_handle::StreamStateHandle;

/// Where one stream lives.
#[derive(Debug)]
pub(crate) enum Source {
    /// Inside `_metadata`: the bytes are the handle's own, borrowed at read time.
    Inline { len: u64 },
    /// A region `[base, base + len)` of a file, kept open once read from.
    File {
        file: OnceLock<File>,
        path: PathBuf,
        base: u64,
        len: u64,
    },
}

impl Source {
    pub(crate) fn len(&self) -> u64 {
        match self {
            Source::Inline { len } | Source::File { len, .. } => *len,
        }
    }

    /// The file, for messages; `None` for an inline stream.
    pub(crate) fn path(&self) -> Option<&Path> {
        match self {
            Source::Inline { .. } => None,
            Source::File { path, .. } => Some(path),
        }
    }

    /// `[start, end)` of the logical stream (clamped to its length). `inline` is the handle's
    /// bytes for an inline stream, which are returned borrowed; a file region is read into `buf`.
    pub(crate) fn read<'s>(
        &self,
        inline: &'s [u8],
        start: u64,
        end: u64,
        buf: &'s mut Vec<u8>,
    ) -> Result<&'s [u8], String> {
        let end = end.min(self.len());
        if end <= start {
            return Ok(&[]);
        }
        match self {
            Source::Inline { .. } => Ok(&inline[start as usize..end as usize]),
            Source::File {
                file, path, base, ..
            } => {
                let file = match file.get() {
                    Some(f) => f,
                    None => {
                        // Two threads may both open it: one handle is kept, the other dropped.
                        let (opened, _) = crate::state_file::open_regular(path)?;
                        let _ = file.set(opened);
                        file.get().expect("set above")
                    }
                };
                buf.clear();
                buf.resize((end - start) as usize, 0);
                read_exact_at(file, buf, base + start)
                    .map_err(|e| format!("read {}: {}", path.display(), e))?;
                Ok(buf.as_slice())
            }
        }
    }

    /// [`read`](Self::read), but a file region is read through a handle of `pool` rather than
    /// one this source keeps: what [`crate::reader::StateReader`] uses, so that the number of
    /// files it holds open is bounded whatever the parallelism.
    pub(crate) fn read_pooled<'s>(
        &self,
        pool: &FilePool,
        inline: &'s [u8],
        start: u64,
        end: u64,
        buf: &'s mut Vec<u8>,
    ) -> Result<&'s [u8], String> {
        let Source::File { path, base, .. } = self else {
            return self.read(inline, start, end, buf);
        };
        let end = end.min(self.len());
        if end <= start {
            return Ok(&[]);
        }
        let file = pool.get(path)?;
        buf.clear();
        buf.resize((end - start) as usize, 0);
        read_exact_at(&file, buf, base + start)
            .map_err(|e| format!("read {}: {}", path.display(), e))?;
        Ok(buf.as_slice())
    }

    /// Close the file until the next read needs it.
    pub(crate) fn close(&mut self) {
        if let Source::File { file, .. } = self {
            file.take();
        }
    }
}

/// An open file: its path, its handle, its last use.
type OpenFile = (PathBuf, Arc<File>, u64);

/// Open files shared by every stream of a reader, at most `capacity` of them, the least recently
/// used closed first. A handle in use by a read stays valid after its eviction (it is an `Arc`).
#[derive(Debug)]
pub(crate) struct FilePool {
    capacity: usize,
    /// `(path, handle, last use)`; a handful of entries, searched linearly.
    open: Mutex<(u64, Vec<OpenFile>)>,
}

impl FilePool {
    pub(crate) fn new(capacity: usize) -> FilePool {
        FilePool {
            capacity: capacity.max(1),
            open: Mutex::new((0, Vec::new())),
        }
    }

    pub(crate) fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity.max(1);
    }

    /// Files currently open.
    pub(crate) fn len(&self) -> usize {
        self.open.lock().map(|g| g.1.len()).unwrap_or(0)
    }

    fn get(&self, path: &Path) -> Result<Arc<File>, String> {
        if let Ok(mut guard) = self.open.lock() {
            let (tick, open) = &mut *guard;
            *tick += 1;
            if let Some(entry) = open.iter_mut().find(|(p, _, _)| p == path) {
                entry.2 = *tick;
                return Ok(entry.1.clone());
            }
        }
        // Opened outside the lock: two threads may both open it, and one handle is kept.
        let file = Arc::new(crate::state_file::open_regular(path)?.0);
        if let Ok(mut guard) = self.open.lock() {
            let (tick, open) = &mut *guard;
            *tick += 1;
            if let Some(entry) = open.iter_mut().find(|(p, _, _)| p == path) {
                entry.2 = *tick;
                return Ok(entry.1.clone());
            }
            while open.len() >= self.capacity {
                let oldest = (0..open.len())
                    .min_by_key(|&i| open[i].2)
                    .expect("non-empty");
                open.swap_remove(oldest);
            }
            open.push((path.to_path_buf(), file.clone(), *tick));
        }
        Ok(file)
    }
}

/// Fill `buf` from `offset` without moving a shared cursor.
#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

/// Fill `buf` from `offset`. `seek_read` may return short, and moves the file cursor, which
/// nothing here relies on.
#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0;
    while done < buf.len() {
        match file.seek_read(&mut buf[done..], offset + done as u64) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)
}

/// The bytes of an inline handle, empty for any other.
pub(crate) fn inline_bytes(delegate: &StreamStateHandle) -> &[u8] {
    match delegate {
        StreamStateHandle::ByteStream(bs) => &bs.data,
        _ => &[],
    }
}

/// Where `delegate`'s bytes are. A file-backed stream is opened here, and must be as long as its
/// handle records ([`crate::state_file::open`]): a missing, unreadable or short file is an `Err`
/// naming it, never a stream that silently yields fewer rows. `Ok(None)`: no stream at all.
///
/// Paths come from `_metadata` and are not trusted: a relative handle must stay inside
/// `savepoint_dir` ([`crate::state_file::relative_path`] refuses absolute paths and `..`), and
/// every file opened — here and on any later reopen — must be a regular file
/// ([`crate::state_file::open_regular`]: a FIFO would block the open forever). Absolute `File` /
/// `Segment` handles are allowed anywhere on the local file system on purpose: that is how
/// checkpoints and externalised savepoints reference their state files.
pub(crate) fn source_for(
    delegate: &StreamStateHandle,
    savepoint_dir: &Path,
) -> Result<Option<Source>, String> {
    match delegate {
        StreamStateHandle::ByteStream(bs) => Ok(Some(Source::Inline {
            len: bs.data.len() as u64,
        })),
        StreamStateHandle::Relative(rel) => {
            let path = crate::state_file::relative_path(savepoint_dir, &rel.relative_path)?;
            let (file, len) = crate::state_file::open(&path, rel.state_size)?;
            Ok(Some(Source::File {
                file: file.into(),
                path,
                base: 0,
                len,
            }))
        }
        StreamStateHandle::File(f) => {
            let path = crate::state_file::local_path(&f.file_path)?;
            let (file, len) = crate::state_file::open(&path, f.state_size)?;
            Ok(Some(Source::File {
                file: file.into(),
                path,
                base: 0,
                len,
            }))
        }
        // A segment handle points at a region of a shared file, and offsets inside it are
        // relative to `start_pos`: FileMergingCheckpointStateOutputStream.getPos (:125-129) on
        // write, FsSegmentDataInputStream.seek (:96-99) on read (release-1.20). No real
        // file-merging savepoint has been read yet.
        StreamStateHandle::Segment(seg) => {
            let path = crate::state_file::local_path(&seg.file_path)?;
            let base = seg.start_pos.max(0) as u64;
            let len = seg.state_size.max(0) as u64;
            // The segment must lie within its file.
            let (file, _) = crate::state_file::open(&path, (base + len) as i64)?;
            Ok(Some(Source::File {
                file: file.into(),
                path,
                base,
                len,
            }))
        }
        StreamStateHandle::KeyGroups(_)
        | StreamStateHandle::Null
        | StreamStateHandle::EmptySegment => Ok(None),
    }
}

/// Read a whole stream into memory, resolving its handle exactly as [`source_for`] does (inline
/// bytes, a file relative to the savepoint, an absolute `file:` path, or a segment of a shared
/// file), with the same checks: a missing, unreadable or truncated file is an error naming it.
///
/// For operator-state streams, which the index reads in full like Flink does on restore
/// (`OperatorStateRestoreOperation`); keyed streams are read one key-group at a time instead.
pub(crate) fn read_whole_stream(
    delegate: &StreamStateHandle,
    savepoint_dir: &Path,
) -> Result<Vec<u8>, String> {
    let Some(src) = source_for(delegate, savepoint_dir)? else {
        return Err("the state handle points at no stream".to_string());
    };
    if let Source::Inline { .. } = src {
        return Ok(inline_bytes(delegate).to_vec());
    }
    let mut buf = Vec::new();
    src.read(&[], 0, src.len(), &mut buf)?;
    Ok(buf)
}
