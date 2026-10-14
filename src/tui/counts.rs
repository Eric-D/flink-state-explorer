//! The key counts cache: the only thing the TUI remembers between runs.
//!
//! It holds **counts only** (one line per keyed operator: hex id and key count), never a key or a
//! value, and lives under the user's cache directory — `$XDG_CACHE_HOME/flink-explorer` or
//! `~/.cache/flink-explorer` on Unix, `%LOCALAPPDATA%\flink-explorer` on Windows (found with
//! `std::env` alone) — **never in the savepoint directory**. One file per savepoint, named after a
//! hash of its canonical path. Its first three lines are a small header — format, `_metadata`
//! size and modification time, the savepoint path — which is all that is read to call it stale;
//! a stale or unreadable file is simply ignored (and replaced after the next complete count).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

const MAGIC: &str = "flink-explorer-counts 1";

/// The cache directory, from the environment; `None` when it cannot be told.
pub fn default_cache_dir() -> Option<PathBuf> {
    let non_empty = |k: &str| {
        std::env::var_os(k)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let base = if cfg!(windows) {
        non_empty("LOCALAPPDATA")
    } else {
        non_empty("XDG_CACHE_HOME").or_else(|| non_empty("HOME").map(|h| h.join(".cache")))
    }?;
    Some(base.join("flink-explorer"))
}

/// 64-bit FNV-1a: a stable file name for a path, without a dependency.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn canonical(savepoint_dir: &Path) -> PathBuf {
    std::fs::canonicalize(savepoint_dir).unwrap_or_else(|_| savepoint_dir.to_path_buf())
}

/// The cache file for `savepoint_dir` under `cache_dir`.
pub fn cache_file(cache_dir: &Path, savepoint_dir: &Path) -> PathBuf {
    let path = canonical(savepoint_dir);
    cache_dir.join(format!(
        "counts-{:016x}.txt",
        fnv1a(path.to_string_lossy().as_bytes())
    ))
}

/// The header that ties a cache to one `_metadata`: size and modification time (nanoseconds).
fn header(savepoint_dir: &Path) -> Option<[String; 3]> {
    let meta = std::fs::metadata(savepoint_dir.join("_metadata")).ok()?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    Some([
        MAGIC.to_string(),
        format!("{} {}", meta.len(), mtime),
        canonical(savepoint_dir).to_string_lossy().into_owned(),
    ])
}

/// The cached counts of `savepoint_dir`, by operator hex id: `None` when there is no cache, or it
/// was written for another `_metadata` (decided from the header, before the body is read).
pub fn load(cache_dir: &Path, savepoint_dir: &Path) -> Option<HashMap<String, usize>> {
    let want = header(savepoint_dir)?;
    let file = std::fs::File::open(cache_file(cache_dir, savepoint_dir)).ok()?;
    let mut lines = BufReader::new(file).lines();
    for expected in &want {
        if lines.next()?.ok()? != *expected {
            return None;
        }
    }
    let mut counts = HashMap::new();
    for line in lines {
        let line = line.ok()?;
        let (hex, n) = line.split_once(' ')?;
        counts.insert(hex.to_string(), n.parse().ok()?);
    }
    Some(counts)
}

/// A side file for `path` no other writer uses at the same time: two explorers on one savepoint
/// (or two saves in one process) each write their own, and the last rename wins whole.
pub fn temp_file(path: &Path) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".{}-{}.partial", std::process::id(), n));
    PathBuf::from(tmp)
}

/// Write the counts of `savepoint_dir` (through a side file of its own, synced, then renamed). Only complete counts
/// should be saved: a count that met an unreadable key-group is a lower bound.
pub fn save(
    cache_dir: &Path,
    savepoint_dir: &Path,
    counts: &[(String, usize)],
) -> Result<PathBuf, String> {
    let head = header(savepoint_dir).ok_or_else(|| "no _metadata".to_string())?;
    std::fs::create_dir_all(cache_dir)
        .map_err(|e| format!("create {}: {}", cache_dir.display(), e))?;
    let path = cache_file(cache_dir, savepoint_dir);
    let tmp = temp_file(&path);
    let write = || -> std::io::Result<()> {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
        for h in &head {
            writeln!(f, "{}", h)?;
        }
        for (hex, n) in counts {
            writeln!(f, "{} {}", hex, n)?;
        }
        f.flush()?;
        // On disk before the rename makes it the cache: a crash then leaves the old file or the
        // new one, never a truncated one under the real name.
        f.get_ref().sync_all()
    };
    let result = write()
        .map_err(|e| format!("write {}: {}", tmp.display(), e))
        .and_then(|()| {
            std::fs::rename(&tmp, &path).map_err(|e| format!("rename {}: {}", tmp.display(), e))
        });
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result.map(|()| path)
}
