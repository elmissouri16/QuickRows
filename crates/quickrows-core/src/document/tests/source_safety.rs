#[test]
fn save_preserves_custom_quote_and_crlf() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.csv");
    std::fs::write(&path, "name;note\r\nalpha;'one;two'\r\n").unwrap();
    let overrides = ParseOverrides {
        delimiter: Some("semicolon".to_string()),
        quote: Some("single".to_string()),
        line_ending: Some("crlf".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    doc.edit_cell(0, 1, "updated;note".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "name;note\r\nalpha;'updated;note'\r\n"
    );
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "updated;note");
}

#[test]
fn save_preserves_cr_terminators_and_embedded_newlines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("classic-mac.csv");
    std::fs::write(&path, b"name,note\ralpha,\"line one\nline two\"\r").unwrap();
    let overrides = ParseOverrides {
        line_ending: Some("cr".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    doc.edit_cell(0, 0, "alpha, edited".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"name,note\r\"alpha, edited\",\"line one\nline two\"\r"
    );
    assert_eq!(doc.metadata().effective.line_ending, "cr");
}

#[test]
fn unicode_paths_round_trip_through_save() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("données-日本語-😀.csv");
    std::fs::write(&path, "name,value\ncafé,1\n").unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(0, 1, "2".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["café", "2"]);
}

#[test]
fn save_preserves_single_byte_source_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("windows-1252.csv");
    std::fs::write(&path, b"name,value\r\ncaf\xe9,one\r\n").unwrap();
    let overrides = ParseOverrides {
        encoding: Some("windows-1252".to_string()),
        line_ending: Some("crlf".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    doc.edit_cell(0, 1, "deux".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"name,value\r\ncaf\xe9,deux\r\n"
    );
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "café");
}

#[test]
fn unrepresentable_legacy_encoding_edits_do_not_replace_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("windows-1252-atomic.csv");
    let original = b"name,value\r\ncaf\xe9,one\r\n".to_vec();
    std::fs::write(&path, &original).unwrap();
    let overrides = ParseOverrides {
        encoding: Some("windows-1252".to_string()),
        line_ending: Some("crlf".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    doc.edit_cell(0, 1, "🧊".to_string()).unwrap();
    assert!(doc
        .save(&path)
        .unwrap_err()
        .contains("cannot be represented"));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(doc.is_dirty());
}

#[test]
fn bulk_row_mutation_is_transactional_on_validation_failure() {
    let (_dir, mut doc) = document("name,value\na,1\nb,2\n");
    assert!(doc
        .set_display_rows_deleted(&[0, usize::MAX], true)
        .is_err());
    assert!(!doc.is_display_row_deleted(0));
    assert!(!doc.is_dirty());
}

#[test]
fn duplicate_row_mutations_are_counted_once() {
    let (_dir, mut doc) = document("name,value\na,1\nb,2\n");

    assert_eq!(doc.set_display_rows_deleted(&[0, 0], true).unwrap(), 1);
    assert!(doc.is_display_row_deleted(0));
    assert_eq!(doc.set_display_rows_deleted(&[0, 0], false).unwrap(), 1);
    assert!(!doc.is_display_row_deleted(0));
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_paths_open_and_save() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let mut name = b"data-".to_vec();
    name.push(0xff);
    name.extend_from_slice(b".csv");
    let path = dir.path().join(std::ffi::OsString::from_vec(name));
    std::fs::write(&path, "name,value\na,1\n").unwrap();
    let mut doc = CsvDocument::open(&path, None, None).unwrap();

    doc.edit_cell(0, 1, "2".to_string()).unwrap();
    doc.save(&path).unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), b"name,value\na,2\n");
}

#[cfg(unix)]
#[test]
fn save_as_uses_normal_creation_permissions() {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let (dir, mut doc) = document("name,value\na,1\n");
    doc.edit_cell(0, 1, "2".to_string()).unwrap();
    let probe = dir.path().join("permission-probe");
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o666)
        .open(&probe)
        .unwrap();
    let expected_mode = std::fs::metadata(&probe).unwrap().permissions().mode() & 0o777;
    let destination = dir.path().join("saved-as.csv");

    doc.save(&destination).unwrap();

    let actual_mode = std::fs::metadata(destination).unwrap().permissions().mode() & 0o777;
    assert_eq!(actual_mode, expected_mode);
}

#[test]
fn prepared_document_fingerprint_uses_raw_utf16_source_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raw-utf16.csv");
    let bytes = utf16_bytes("name,value\r\nalpha,1\r\n", true, true);
    std::fs::write(&path, &bytes).unwrap();

    let document = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();

    assert_eq!(document.source_fingerprint.len, bytes.len() as u64);
    assert_eq!(
        document.source_fingerprint.content_hash,
        *blake3::hash(&bytes).as_bytes()
    );
    assert_eq!(
        document.source_fingerprint,
        file_fingerprint(&path).unwrap()
    );
    assert_ne!(document.data_path, path);
}

#[test]
fn source_pipeline_failures_prefer_a_typed_change_after_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rewritten.csv");
    std::fs::write(&path, "name,value\nalpha,1\n").unwrap();
    let expected = file_fingerprint(&path).unwrap();
    std::fs::write(&path, "name,value\nbravo,9\n").unwrap();

    let error = prefer_source_changed(
        &path,
        expected,
        &|| false,
        QuickRowsError::invalid_csv("interrupted source parse"),
        "CSV changed while reading",
    );

    assert_eq!(error.kind(), crate::ErrorKind::SourceChanged);
    assert_eq!(error.to_string(), "CSV changed while reading");
}

#[test]
fn open_rejects_source_change_during_indexing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("changing.csv");
    std::fs::write(&path, "name,value\nalpha,1\nbeta,2\n").unwrap();
    let original_fingerprint = file_fingerprint(&path).unwrap();
    let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let changed = std::cell::Cell::new(false);
    let source_path = path.clone();
    let progress = |_| {
        if !changed.replace(true) {
            std::fs::write(&source_path, "name,value\nbravo,8\nzeta,9\n").unwrap();
            let source = std::fs::OpenOptions::new()
                .write(true)
                .open(&source_path)
                .unwrap();
            source
                .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                .unwrap();
        }
    };

    let error = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        Some(&progress),
    )
    .err()
    .expect("open should reject a source change");

    assert!(changed.get());
    let replacement_fingerprint = file_fingerprint(&path).unwrap();
    assert_eq!(replacement_fingerprint.len, original_fingerprint.len);
    assert_eq!(
        replacement_fingerprint.modified,
        original_fingerprint.modified
    );
    assert_ne!(
        replacement_fingerprint.content_hash,
        original_fingerprint.content_hash
    );
    assert_eq!(error.kind(), crate::ErrorKind::SourceChanged);
    assert!(error.contains("changed on disk while it was being opened"));
}

#[test]
fn open_classifies_source_deletion_during_indexing_as_source_changed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deleted-during-open.csv");
    std::fs::write(&path, "name,value\nalpha,1\nbeta,2\n").unwrap();
    let deleted = std::cell::Cell::new(false);
    let source_path = path.clone();
    let progress = |_| {
        if !deleted.replace(true) {
            std::fs::remove_file(&source_path).unwrap();
        }
    };

    let error = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        Some(&progress),
    )
    .err()
    .expect("open should reject a deleted source");

    assert!(deleted.get());
    assert_eq!(error.kind(), crate::ErrorKind::SourceChanged);
    assert!(error.contains("changed or became inaccessible"));
}

#[cfg(unix)]
#[test]
fn open_rejects_symlink_retarget_during_indexing() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    let link = dir.path().join("linked.csv");
    std::fs::write(&first, "name,value\nfirst,1\n").unwrap();
    std::fs::write(&second, "name,value\nother,2\n").unwrap();
    symlink(&first, &link).unwrap();
    let retargeted = std::cell::Cell::new(false);
    let link_path = link.clone();
    let second_path = second.clone();
    let progress = |_| {
        if !retargeted.replace(true) {
            std::fs::remove_file(&link_path).unwrap();
            symlink(&second_path, &link_path).unwrap();
        }
    };

    let error = CsvDocument::open(
        &link,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        Some(&progress),
    )
    .err()
    .expect("open should reject a retargeted source");

    assert!(retargeted.get());
    assert!(error.contains("changed on disk while it was being opened"));
}

#[test]
fn ordinary_open_does_not_create_a_source_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct.csv");
    std::fs::write(&path, "name,value\na,1\n").unwrap();
    let visible_sidecar_observed = std::cell::Cell::new(false);
    let progress = |_| {
        visible_sidecar_observed.set(dir.path().read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("quickrows-")
        }));
    };

    let doc = CsvDocument::open(&path, None, Some(&progress)).unwrap();

    assert!(!visible_sidecar_observed.get());
    assert_eq!(doc.data_path, path);
    assert!(doc._prepared_source.is_none());
    assert_eq!(dir.path().read_dir().unwrap().count(), 1);
}
