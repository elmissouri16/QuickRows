use super::super::store::write_cache_atomically;
use crate::QuickRowsError;
use std::io::Write;

#[test]
fn failed_atomic_cache_write_preserves_existing_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("cache.bin");
    std::fs::write(&path, b"existing cache").expect("write existing cache");

    let error = write_cache_atomically(&path, |writer| {
        writer
            .write_all(b"partial replacement")
            .map_err(QuickRowsError::from)?;
        Err(QuickRowsError::cache_corrupt(
            "injected cache write failure",
        ))
    })
    .expect_err("cache write should fail");

    assert_eq!(error.to_string(), "injected cache write failure");
    assert_eq!(error.kind(), crate::ErrorKind::CacheCorrupt);
    assert_eq!(std::fs::read(&path).unwrap(), b"existing cache");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn atomic_cache_write_replaces_existing_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("cache.bin");
    std::fs::write(&path, b"existing cache").expect("write existing cache");

    write_cache_atomically(&path, |writer| {
        writer
            .write_all(b"replacement cache")
            .map_err(QuickRowsError::from)
    })
    .expect("replace cache");

    assert_eq!(std::fs::read(&path).unwrap(), b"replacement cache");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn failed_atomic_persist_removes_the_temporary_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let occupied = dir.path().join("occupied");
    std::fs::create_dir(&occupied).expect("create occupied destination");

    assert!(
        write_cache_atomically(&occupied, |writer| {
            writer
                .write_all(b"replacement cache")
                .map_err(QuickRowsError::from)
        })
        .is_err()
    );

    assert!(occupied.is_dir());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}
