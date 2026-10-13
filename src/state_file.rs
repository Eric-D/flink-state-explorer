//! Opening the files that a savepoint's state handles point to.
//!
//! A missing, unreadable or truncated state file must surface as an error naming the file, never
//! as "no state": read as an empty buffer, it used to make a subtask's keys vanish without a word
//! (issue #16). The index (`index::builder`) and the streaming export (`export_state`) both open
//! state files through here, so they report the same errors in the same words.

use std::fs::File;
use std::io::Read;
use std::path::Path;

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
