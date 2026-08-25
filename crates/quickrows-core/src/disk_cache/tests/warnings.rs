use super::super::*;
use crate::csv::ParseWarning;

#[test]
fn content_changes_invalidate_cache_even_with_matching_metadata() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"a,b\n1,2\n").expect("write first csv");
    let first = cache_key(&file_path, None).expect("first key");
    let offsets_path = offsets_cache_path(dir.path(), first);
    write_offsets_cache(&offsets_path, first, &[4]).expect("write offsets");

    std::fs::write(&file_path, b"a,b\n9,8\n").expect("write replacement csv");
    let mut replacement = cache_key(&file_path, None).expect("second key");
    assert_ne!(first.content_hash, replacement.content_hash);

    // Simulate a filesystem with a coarse or explicitly preserved mtime
    // and a colliding cache path. The cache header still rejects the bytes.
    replacement.hash = first.hash;
    replacement.modified = first.modified;
    assert!(
        read_offsets_cache(&offsets_path, replacement)
            .expect("read offsets")
            .is_none()
    );
}

#[test]
fn warnings_cache_rejects_valid_json_with_modified_diagnostics() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"a\n1\n").unwrap();
    let key = cache_key(&file_path, None).unwrap();
    let warnings_path = warnings_cache_path(dir.path(), key);
    let warnings = vec![ParseWarning {
        record: Some(123),
        line: Some(4),
        byte: Some(8),
        field: Some(1),
        kind: "test".to_string(),
        message: "test warning".to_string(),
        expected_len: Some(2),
        len: Some(3),
    }];
    write_warnings_cache(&warnings_path, key, &warnings).unwrap();
    assert_eq!(
        read_warnings_cache(&warnings_path, key).unwrap().unwrap()[0].record,
        Some(123)
    );

    let bytes = std::fs::read(&warnings_path).unwrap();
    let json = String::from_utf8(bytes).unwrap();
    let modified = json.replacen("\"record\":123", "\"record\":124", 1);
    assert_ne!(modified, json);
    std::fs::write(&warnings_path, modified).unwrap();

    assert!(read_warnings_cache(&warnings_path, key).unwrap().is_none());
}

#[test]
fn oversized_warning_cache_is_rejected_before_reading() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"a\n1\n").unwrap();
    let key = cache_key(&file_path, None).unwrap();
    let warnings_path = warnings_cache_path(dir.path(), key);
    let file = std::fs::File::create(&warnings_path).unwrap();
    file.set_len(4 * 1024 * 1024 + 1).unwrap();

    assert!(read_warnings_cache(&warnings_path, key).unwrap().is_none());
}
