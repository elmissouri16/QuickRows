use super::super::snapshot::{
    SnapshotCapture, copy_snapshot, snapshot_csv_source_with_capture, try_reflink_snapshot,
};
use super::super::*;
use crate::disk_cache::file_fingerprint;
use std::path::PathBuf;

#[test]
fn snapshot_copy_hashes_multiple_buffers_and_partial_tail() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.csv");
    let destination_path = dir.path().join("destination.csv");
    let bytes = (0..SOURCE_SNAPSHOT_BUFFER_BYTES * 2 + 137)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&source_path, &bytes).unwrap();
    let mut source = std::fs::File::open(&source_path).unwrap();
    let mut destination = std::fs::File::create(&destination_path).unwrap();

    let (copied, content_hash) = copy_snapshot(&mut source, &mut destination, &|| false).unwrap();
    drop(destination);

    assert_eq!(copied, bytes.len() as u64);
    assert_eq!(content_hash, *blake3::hash(&bytes).as_bytes());
    assert_eq!(std::fs::read(destination_path).unwrap(), bytes);
}

#[test]
fn failed_capture_with_owned_destination_falls_back_to_stream_copy() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.csv");
    let bytes = vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES + 17];
    std::fs::write(&source_path, &bytes).unwrap();
    let capture = |_: &std::fs::File, destination: &Path, _: u64| {
        let snapshot = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(destination)?;
        Ok(SnapshotCapture::Retry(snapshot))
    };

    let snapshot = snapshot_csv_source_with_capture(&source_path, &|| false, &capture).unwrap();

    assert_eq!(std::fs::read(snapshot.temporary.path()).unwrap(), bytes);
    assert_eq!(
        snapshot.fingerprint.content_hash,
        *blake3::hash(&bytes).as_bytes()
    );
    drop(snapshot);
    assert!(snapshot_artifacts(dir.path()).is_empty());
}

#[test]
fn stream_capture_rejects_a_detectable_change_and_removes_the_temporary() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.csv");
    let bytes = vec![b'a'; SOURCE_SNAPSHOT_BUFFER_BYTES * 2 + 17];
    std::fs::write(&source_path, &bytes).unwrap();
    let original_modified = std::fs::metadata(&source_path).unwrap().modified().unwrap();
    let changed_modified = original_modified
        .checked_add(std::time::Duration::from_secs(60))
        .unwrap();
    let checks = std::cell::Cell::new(0usize);
    let changed = std::cell::Cell::new(false);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 2 {
            let mut replacement = bytes.clone();
            replacement[..SOURCE_SNAPSHOT_BUFFER_BYTES].fill(b'z');
            std::fs::write(&source_path, replacement).unwrap();
            let source = std::fs::OpenOptions::new()
                .write(true)
                .open(&source_path)
                .unwrap();
            source
                .set_times(std::fs::FileTimes::new().set_modified(changed_modified))
                .unwrap();
            changed.set(true);
        }
        false
    };
    let force_copy = |_: &std::fs::File, _: &Path, _: u64| Ok(SnapshotCapture::Unsupported);

    let error = snapshot_csv_source_with_capture(&source_path, &is_cancelled, &force_copy)
        .err()
        .expect("capture should reject a detectable source change");

    assert!(changed.get());
    assert_eq!(error.kind(), crate::ErrorKind::SourceChanged);
    assert_eq!(
        error.to_string(),
        "CSV changed on disk while it was being captured"
    );
    assert!(snapshot_artifacts(dir.path()).is_empty());
}

#[test]
fn stream_capture_fingerprints_exact_observed_bytes_when_mtime_is_restored() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.csv");
    let mut original = vec![b'a'; SOURCE_SNAPSHOT_BUFFER_BYTES];
    original.extend(vec![b'b'; SOURCE_SNAPSHOT_BUFFER_BYTES]);
    original.extend(vec![b'c'; 17]);
    std::fs::write(&source_path, &original).unwrap();
    let original_modified = std::fs::metadata(&source_path).unwrap().modified().unwrap();
    let mut replacement = original.clone();
    replacement[..SOURCE_SNAPSHOT_BUFFER_BYTES].fill(b'z');
    replacement[SOURCE_SNAPSHOT_BUFFER_BYTES..SOURCE_SNAPSHOT_BUFFER_BYTES * 2].fill(b'y');
    let checks = std::cell::Cell::new(0usize);
    let changed = std::cell::Cell::new(false);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        if current == 2 {
            std::fs::write(&source_path, &replacement).unwrap();
            let source = std::fs::OpenOptions::new()
                .write(true)
                .open(&source_path)
                .unwrap();
            source
                .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                .unwrap();
            changed.set(true);
        }
        false
    };
    let force_copy = |_: &std::fs::File, _: &Path, _: u64| Ok(SnapshotCapture::Unsupported);

    let snapshot =
        snapshot_csv_source_with_capture(&source_path, &is_cancelled, &force_copy).unwrap();
    let captured = std::fs::read(snapshot.temporary.path()).unwrap();
    let mut observed = original;
    observed[SOURCE_SNAPSHOT_BUFFER_BYTES..SOURCE_SNAPSHOT_BUFFER_BYTES * 2].fill(b'y');

    assert!(changed.get());
    assert_eq!(captured, observed);
    assert_eq!(snapshot.fingerprint.len, captured.len() as u64);
    assert_eq!(
        snapshot.fingerprint.content_hash,
        *blake3::hash(&captured).as_bytes()
    );
    assert_ne!(
        snapshot.fingerprint,
        file_fingerprint(&source_path).unwrap()
    );
}

pub(super) fn snapshot_artifacts(directory: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(directory)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("quickrows-source-"))
        })
        .collect()
}

#[test]
fn cancelled_snapshot_copy_removes_the_partial_temporary() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.csv");
    std::fs::write(&source_path, vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 4]).unwrap();
    let checks = std::cell::Cell::new(0usize);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        current >= 3
    };
    let force_copy = |_: &std::fs::File, _: &Path, _: u64| Ok(SnapshotCapture::Unsupported);

    let error = snapshot_csv_source_with_capture(&source_path, &is_cancelled, &force_copy)
        .err()
        .expect("stream capture should be cancelled");

    assert_eq!(error.kind(), crate::ErrorKind::Cancelled);
    assert_eq!(error.to_string(), "Operation cancelled");
    assert!(checks.get() >= 4);
    assert!(snapshot_artifacts(dir.path()).is_empty());
}

#[test]
fn cancelled_snapshot_hash_removes_the_captured_temporary() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.csv");
    std::fs::write(&source_path, vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 4]).unwrap();
    let checks = std::cell::Cell::new(0usize);
    let is_cancelled = || {
        let current = checks.get();
        checks.set(current + 1);
        current >= 3
    };
    let capture = |source: &std::fs::File, destination: &Path, _: u64| {
        let mut source = source.try_clone()?;
        source.seek(std::io::SeekFrom::Start(0))?;
        let mut snapshot = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(destination)?;
        std::io::copy(&mut source, &mut snapshot)?;
        snapshot.seek(std::io::SeekFrom::Start(0))?;
        Ok(SnapshotCapture::Captured(snapshot))
    };

    let error = snapshot_csv_source_with_capture(&source_path, &is_cancelled, &capture)
        .err()
        .expect("captured-file hashing should be cancelled");

    assert_eq!(error.kind(), crate::ErrorKind::Cancelled);
    assert_eq!(error.to_string(), "Operation cancelled");
    assert!(checks.get() >= 4);
    assert!(snapshot_artifacts(dir.path()).is_empty());
}

#[cfg(unix)]
#[test]
fn snapshot_capture_uses_the_open_referent_through_a_symlink_aba() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    let link = dir.path().join("linked.csv");
    std::fs::write(&first, "name,value\nfirst,1\n").unwrap();
    std::fs::write(&second, "name,value\nother,2\n").unwrap();
    symlink(&first, &link).unwrap();
    let capture = |source: &std::fs::File, destination: &Path, len: u64| {
        std::fs::remove_file(&link)?;
        symlink(&second, &link)?;
        let result = try_reflink_snapshot(source, destination, len);
        std::fs::remove_file(&link)?;
        symlink(&first, &link)?;
        result
    };

    let snapshot = snapshot_csv_source_with_capture(&link, &|| false, &capture).unwrap();

    assert_eq!(
        std::fs::read(snapshot.temporary.path()).unwrap(),
        std::fs::read(first).unwrap()
    );
}

#[cfg(unix)]
#[test]
fn snapshot_temporary_is_anchored_across_parent_symlink_aba() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first_parent = dir.path().join("first-parent");
    let second_parent = dir.path().join("second-parent");
    let parent_link = dir.path().join("parent-link");
    std::fs::create_dir(&first_parent).unwrap();
    std::fs::create_dir(&second_parent).unwrap();
    std::fs::write(first_parent.join("source.csv"), "name,value\nfirst,1\n").unwrap();
    std::fs::write(second_parent.join("source.csv"), "name,value\nother,2\n").unwrap();
    symlink(&first_parent, &parent_link).unwrap();
    let path = parent_link.join("source.csv");
    let capture = |source: &std::fs::File, destination: &Path, _: u64| {
        std::fs::remove_file(&parent_link)?;
        symlink(&second_parent, &parent_link)?;
        let mut source = source.try_clone()?;
        source.seek(std::io::SeekFrom::Start(0))?;
        let mut snapshot = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(destination)?;
        std::io::copy(&mut source, &mut snapshot)?;
        snapshot.seek(std::io::SeekFrom::Start(0))?;
        std::fs::remove_file(&parent_link)?;
        symlink(&first_parent, &parent_link)?;
        Ok(SnapshotCapture::Captured(snapshot))
    };

    let snapshot = snapshot_csv_source_with_capture(&path, &|| false, &capture).unwrap();
    let snapshot_path = snapshot.temporary.path().to_path_buf();
    let snapshot_name = snapshot_path.file_name().unwrap().to_owned();

    assert_eq!(
        snapshot_path.parent(),
        Some(std::fs::canonicalize(&first_parent).unwrap().as_path())
    );
    assert_eq!(
        std::fs::read(&snapshot_path).unwrap(),
        std::fs::read(first_parent.join("source.csv")).unwrap()
    );
    assert!(!second_parent.join(&snapshot_name).exists());
    drop(snapshot);
    assert!(!first_parent.join(&snapshot_name).exists());
    assert!(!second_parent.join(&snapshot_name).exists());
}
