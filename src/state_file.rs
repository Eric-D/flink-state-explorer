//! Opening the files that a savepoint's state handles point to.
//!
//! A missing, unreadable or truncated state file must surface as an error naming the file, never
//! as "no state": read as an empty buffer, it used to make a subtask's keys vanish without a word
//! (issue #16). The index (`index::builder`), the streaming export (`export_state`) and the
//! [`crate::reader::StateReader`] all open state files through here, so they report the same errors
//! in the same words.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

/// The local path of an absolute state-file handle. Flink writes `Path.toString()`, which keeps the
/// scheme (`file:/abs/x`, `file:///abs/x`); a remote file system (`s3://`, `hdfs://`, …) cannot be
/// read here, and saying so beats the misleading "No such file or directory" it would give.
pub fn local_path(flink_path: &str) -> Result<PathBuf, String> {
    if let Some(rest) = flink_path.strip_prefix("file:") {
        // file:/abs, file://abs (authority-less, as Flink writes local paths), file:///abs.
        let rest = rest.trim_start_matches('/');
        return Ok(PathBuf::from(format!("/{}", rest)));
    }
    match flink_path.split_once("://") {
        Some((scheme, _)) if !scheme.is_empty() && scheme.chars().all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.') => Err(format!(
            "{}: remote file system ({}), not readable locally; copy the savepoint to a local directory",
            flink_path, scheme
        )),
        _ => Ok(PathBuf::from(flink_path)),
    }
}

/// Open `path`, check it is readable and at least `expected_len` bytes long, and return it with
/// its actual length.
///
/// `expected_len` is the `stateSize` the handle recorded. Flink sets it to the stream position
/// when the state file was closed (`FsCheckpointStreamFactory.java:396-410`, release-1.20) and
/// writes it next to the path (`MetadataV2V3SerializerBase.java:729-732` for a relative handle,
/// `:747-750` for an absolute one), so a file shorter than that has lost data. A negative size
/// means Flink could not tell (`getPos()` threw and left `-1`): only existence and readability
/// are checked then.
pub fn open(path: &Path, expected_len: i64) -> Result<(File, u64), String> {
    let file = File::open(path).map_err(|e| io_error(path, &e))?;
    let len = file.metadata().map_err(|e| io_error(path, &e))?.len();
    check_len(path, len, expected_len)?;
    Ok((file, len))
}

/// Read the whole of `path`, checked like [`open`].
pub fn read(path: &Path, expected_len: i64) -> Result<Vec<u8>, String> {
    let (mut file, len) = open(path, expected_len)?;
    let mut data = Vec::with_capacity(len as usize);
    file.read_to_end(&mut data)
        .map_err(|e| io_error(path, &e))?;
    // The file may have shrunk between the stat and the read.
    check_len(path, data.len() as u64, expected_len)?;
    Ok(data)
}

/// A second length check that does not depend on `stateSize` (which may be `-1`): every
/// key-group must start inside the stream. `what` names the stream in the error.
pub fn check_offsets(
    what: &str,
    offsets: &[i64],
    start_key_group: i32,
    len: u64,
) -> Result<(), String> {
    if let Some((i, &off)) = offsets
        .iter()
        .enumerate()
        .filter(|(_, &o)| o > 0)
        .max_by_key(|(_, &o)| o)
    {
        if off as u64 >= len {
            return Err(format!(
                "{}: truncated: key-group {} starts at byte {}, the stream has {} bytes",
                what,
                start_key_group + i as i32,
                off,
                len
            ));
        }
    }
    Ok(())
}

/// `"<path>: <OS error>"`, the form every state-file I/O error takes.
pub fn io_error(path: &Path, e: &std::io::Error) -> String {
    format!("{}: {}", path.display(), e)
}

fn check_len(path: &Path, actual: u64, expected: i64) -> Result<(), String> {
    if expected >= 0 && actual < expected as u64 {
        return Err(format!(
            "{}: truncated: {} of {} bytes",
            path.display(),
            actual,
            expected
        ));
    }
    Ok(())
}

/// `errors` joined into one line: the first `max`, then how many were left out.
pub fn summarize(errors: &[String], max: usize) -> String {
    let mut s = errors
        .iter()
        .take(max)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("; ");
    if errors.len() > max {
        s.push_str(&format!("; … and {} more", errors.len() - max));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_path_strips_the_file_scheme_and_refuses_remote_ones() {
        assert_eq!(local_path("file:/a/b").unwrap(), PathBuf::from("/a/b"));
        assert_eq!(local_path("file:///a/b").unwrap(), PathBuf::from("/a/b"));
        assert_eq!(local_path("/a/b").unwrap(), PathBuf::from("/a/b"));
        let e = local_path("s3://bucket/sp/x").unwrap_err();
        assert!(
            e.contains("s3://bucket/sp/x") && e.contains("remote file system (s3)"),
            "{}",
            e
        );
    }

    fn temp_file(tag: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "flink-explorer-state-file-{}-{}",
            tag,
            std::process::id()
        ));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn a_complete_file_reads_back() {
        let path = temp_file("ok", b"0123456789");
        assert_eq!(read(&path, 10).unwrap(), b"0123456789");
        // Longer than recorded is not data loss.
        assert_eq!(read(&path, 4).unwrap().len(), 10);
        // Flink's "size unknown".
        assert_eq!(read(&path, -1).unwrap().len(), 10);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_short_file_is_truncated() {
        let path = temp_file("short", b"0123");
        let err = read(&path, 10).unwrap_err();
        assert!(err.starts_with(&path.display().to_string()), "{}", err);
        assert!(err.ends_with("truncated: 4 of 10 bytes"), "{}", err);
        assert_eq!(open(&path, 10).unwrap_err(), err);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_missing_file_names_its_path() {
        let path = std::env::temp_dir().join("flink-explorer-state-file-that-does-not-exist");
        let err = read(&path, 0).unwrap_err();
        assert!(err.starts_with(&path.display().to_string()), "{}", err);
    }

    #[test]
    fn summaries_are_capped() {
        let errors: Vec<String> = (0..5).map(|i| format!("e{}", i)).collect();
        assert_eq!(summarize(&errors, 10), "e0; e1; e2; e3; e4");
        assert_eq!(summarize(&errors, 2), "e0; e1; … and 3 more");
    }
}
