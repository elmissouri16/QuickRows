use super::super::fingerprint::{
    file_fingerprint_with_cancellation, file_fingerprint_with_identity,
};
use super::super::*;
use crate::disk_cache::file_fingerprint;

#[test]
fn source_validation_detects_same_length_change_with_restored_mtime() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.csv");
    let original = b"name,value\nalpha,1\n";
    let replacement = b"name,value\nbravo,9\n";
    assert_eq!(original.len(), replacement.len());
    std::fs::write(&path, original).unwrap();
    let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let opened_fingerprint = file_fingerprint_cancellable(&path, &|| false).unwrap();

    std::fs::write(&path, replacement).unwrap();
    let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    source
        .set_times(std::fs::FileTimes::new().set_modified(original_modified))
        .unwrap();
    drop(source);

    let replacement_fingerprint = file_fingerprint(&path).unwrap();
    assert_eq!(
        replacement_fingerprint,
        file_fingerprint_cancellable(&path, &|| false).unwrap()
    );
    assert_eq!(replacement_fingerprint.len, opened_fingerprint.len);
    assert_eq!(
        replacement_fingerprint.modified,
        opened_fingerprint.modified
    );
    assert_ne!(
        replacement_fingerprint.content_hash,
        opened_fingerprint.content_hash
    );
    assert_eq!(
        opened_fingerprint.content_hash,
        *blake3::hash(original).as_bytes()
    );
}

#[test]
fn cancelled_final_live_fingerprint_stops_between_buffers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.csv");
    std::fs::write(&path, vec![b'x'; SOURCE_IO_BUFFER_BYTES * 4]).unwrap();
    let checks = std::cell::Cell::new(0usize);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        current >= 3
    };

    let error = file_fingerprint_with_cancellation(&path, &is_cancelled)
        .expect_err("live fingerprint should be cancelled");

    assert_eq!(error.kind(), crate::ErrorKind::Cancelled);
    assert_eq!(error.to_string(), "Operation cancelled");
    assert!(checks.get() >= 4);
}

#[cfg(unix)]
#[test]
fn final_live_fingerprint_detects_restored_mtime_write_behind_hash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.csv");
    let original = vec![b'x'; SOURCE_IO_BUFFER_BYTES * 3 + 17];
    std::fs::write(&path, &original).unwrap();
    let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let mut replacement = original.clone();
    replacement[..SOURCE_IO_BUFFER_BYTES].fill(b'z');
    let checks = std::cell::Cell::new(0usize);
    let changed = std::cell::Cell::new(false);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 2 {
            std::fs::write(&path, &replacement).unwrap();
            let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            source
                .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                .unwrap();
            changed.set(true);
        }
        false
    };

    let error = file_fingerprint_with_cancellation(&path, &is_cancelled)
        .expect_err("live fingerprint should reject a write behind its read cursor");

    assert!(changed.get());
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        original.len() as u64
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        original_modified
    );
    assert!(error.contains("changed on disk while it was being fingerprinted"));
}

#[cfg(unix)]
#[test]
fn final_live_fingerprint_detects_write_after_hash_before_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.csv");
    let original = vec![b'x'; SOURCE_IO_BUFFER_BYTES * 2];
    std::fs::write(&path, &original).unwrap();
    let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let mut replacement = original;
    replacement[..SOURCE_IO_BUFFER_BYTES].fill(b'z');
    let checks = std::cell::Cell::new(0usize);
    let changed = std::cell::Cell::new(false);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 5 {
            std::fs::write(&path, &replacement).unwrap();
            let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            source
                .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                .unwrap();
            changed.set(true);
        }
        false
    };

    let error = file_fingerprint_with_cancellation(&path, &is_cancelled)
        .expect_err("live fingerprint should reject a post-hash write");

    assert!(changed.get());
    assert!(error.contains("changed on disk while it was being fingerprinted"));
}

#[cfg(unix)]
#[test]
fn identity_unavailable_fallback_rehashes_a_retargeted_path() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    let link = dir.path().join("linked.csv");
    let first_bytes = vec![b'a'; SOURCE_IO_BUFFER_BYTES * 3 + 17];
    let second_bytes = vec![b'b'; first_bytes.len()];
    std::fs::write(&first, &first_bytes).unwrap();
    std::fs::write(&second, &second_bytes).unwrap();
    let first_modified = std::fs::metadata(&first).unwrap().modified().unwrap();
    let second_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&second)
        .unwrap();
    second_file
        .set_times(std::fs::FileTimes::new().set_modified(first_modified))
        .unwrap();
    drop(second_file);
    assert_eq!(
        metadata_modified(&std::fs::metadata(&first).unwrap()),
        metadata_modified(&std::fs::metadata(&second).unwrap())
    );
    symlink(&first, &link).unwrap();
    let checks = std::cell::Cell::new(0usize);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 2 {
            std::fs::remove_file(&link).unwrap();
            symlink(&second, &link).unwrap();
        }
        false
    };
    let no_identity = |_: &std::fs::File| Ok(None);

    let error = file_fingerprint_with_identity(&link, &is_cancelled, &no_identity)
        .expect_err("identity fallback should reject different live content");

    assert!(error.contains("changed on disk while it was being fingerprinted"));
}

#[cfg(unix)]
#[test]
fn identity_fallback_rechecks_path_after_its_rehash() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    let link = dir.path().join("linked.csv");
    let first_bytes = vec![b'a'; SOURCE_IO_BUFFER_BYTES * 2];
    let second_bytes = vec![b'b'; first_bytes.len()];
    std::fs::write(&first, &first_bytes).unwrap();
    std::fs::write(&second, &second_bytes).unwrap();
    let first_modified = std::fs::metadata(&first).unwrap().modified().unwrap();
    let second_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&second)
        .unwrap();
    second_file
        .set_times(std::fs::FileTimes::new().set_modified(first_modified))
        .unwrap();
    drop(second_file);
    assert_eq!(
        metadata_modified(&std::fs::metadata(&first).unwrap()),
        metadata_modified(&std::fs::metadata(&second).unwrap())
    );
    symlink(&first, &link).unwrap();
    let checks = std::cell::Cell::new(0usize);
    let retargeted = std::cell::Cell::new(false);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 8 {
            std::fs::remove_file(&link).unwrap();
            symlink(&second, &link).unwrap();
            retargeted.set(true);
        }
        false
    };
    let no_identity = |_: &std::fs::File| Ok(None);

    let error = file_fingerprint_with_identity(&link, &is_cancelled, &no_identity)
        .expect_err("fallback should reject a retarget during its hash");

    assert!(retargeted.get());
    assert!(error.contains("changed on disk while it was being fingerprinted"));
}

#[cfg(unix)]
#[test]
fn final_live_fingerprint_detects_retarget_during_hashing() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    let link = dir.path().join("linked.csv");
    let bytes = vec![b'x'; SOURCE_IO_BUFFER_BYTES * 3 + 17];
    std::fs::write(&first, &bytes).unwrap();
    std::fs::write(&second, &bytes).unwrap();
    let first_modified = std::fs::metadata(&first).unwrap().modified().unwrap();
    let second_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&second)
        .unwrap();
    second_file
        .set_times(std::fs::FileTimes::new().set_modified(first_modified))
        .unwrap();
    drop(second_file);
    assert_eq!(
        metadata_modified(&std::fs::metadata(&first).unwrap()),
        metadata_modified(&std::fs::metadata(&second).unwrap())
    );
    symlink(&first, &link).unwrap();
    let checks = std::cell::Cell::new(0usize);
    let retargeted = std::cell::Cell::new(false);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 2 {
            std::fs::remove_file(&link).unwrap();
            symlink(&second, &link).unwrap();
            retargeted.set(true);
        }
        false
    };

    let error = file_fingerprint_with_cancellation(&link, &is_cancelled)
        .expect_err("live fingerprint should reject a new path referent");

    assert!(retargeted.get());
    assert!(error.contains("changed on disk while it was being fingerprinted"));
}
