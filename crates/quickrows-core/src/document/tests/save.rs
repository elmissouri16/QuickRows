#[test]
fn edits_deletes_and_saves() {
    let (dir, mut doc) = document("name,value\na,1\nb,2\n");
    doc.edit_cell(0, 1, "9".to_string()).unwrap();
    doc.delete_display_row(1).unwrap();
    assert!(doc.is_dirty());
    let output = dir.path().join("saved.csv");
    doc.save(&output).unwrap();
    assert_eq!(
        std::fs::read_to_string(output).unwrap(),
        "name,value\na,9\n"
    );
    assert!(!doc.is_dirty());
}

#[test]
fn save_tracks_committed_raw_bytes_and_keeps_an_immutable_backing() {
    let (dir, mut doc) = document("name,value\na,1\nb,2\n");
    doc.edit_cell(0, 1, "updated".to_string()).unwrap();
    let output = dir.path().join("saved-fingerprint.csv");

    doc.save(&output).unwrap();

    assert_eq!(doc.source_fingerprint, file_fingerprint(&output).unwrap());
    assert_ne!(doc.data_path, output);
    assert!(doc._prepared_source.is_some());
    std::fs::write(&output, "name,value\nexternal,9\n").unwrap();
    assert_eq!(
        doc.display_rows(0, 2).unwrap(),
        vec![
            (0, vec!["a".to_string(), "updated".to_string()]),
            (1, vec!["b".to_string(), "2".to_string()]),
        ]
    );
}

#[test]
fn save_flushes_multiple_output_buffers() {
    const ROW_COUNT: usize = 20_000;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("buffered.csv");
    let mut contents = String::with_capacity(SAVE_IO_BUFFER_BYTES * 3);
    let payload = "x".repeat(128);
    contents.push_str("id,value\n");
    for row in 0..ROW_COUNT {
        contents.push_str(&row.to_string());
        contents.push(',');
        contents.push_str(&payload);
        contents.push('\n');
    }
    std::fs::write(&path, contents).unwrap();
    let mut document = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    document
        .edit_cell(ROW_COUNT - 1, 1, "updated".to_string())
        .unwrap();
    let output = dir.path().join("buffered-output.csv");

    document.save(&output).unwrap();

    assert!(std::fs::metadata(output).unwrap().len() > (SAVE_IO_BUFFER_BYTES * 2) as u64);
    assert_eq!(
        document.display_rows(ROW_COUNT - 1, 1).unwrap()[0].1[1],
        "updated"
    );
}

#[test]
fn serializes_display_rows_with_csv_settings_and_without_trailing_terminator() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("semicolon.csv");
    std::fs::write(
        &path,
        "name;note\r\nalpha;'one;two'\r\nbeta;'line\r\nbreak'\r\n",
    )
    .unwrap();
    let overrides = ParseOverrides {
        delimiter: Some("semicolon".to_string()),
        quote: Some("single".to_string()),
        line_ending: Some("crlf".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let doc = CsvDocument::open(path, Some(overrides), None).unwrap();

    assert_eq!(
        doc.serialize_display_rows(&[0, 1]).unwrap(),
        "alpha;'one;two'\r\nbeta;'line\r\nbreak'"
    );
}

#[test]
fn serialization_applies_sort_edits_and_deleted_rows() {
    let (_dir, mut doc) = document("name,value\nbeta,2\nalpha,1\ngamma,3\n");
    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    }))
    .unwrap();
    doc.edit_cell(0, 1, "edited, value".to_string()).unwrap();
    doc.delete_display_row(1).unwrap();

    assert_eq!(
        doc.serialize_display_rows(&[2, 0, 1]).unwrap(),
        "gamma,3\nalpha,\"edited, value\""
    );
}

#[test]
fn save_can_atomically_replace_the_open_file() {
    let (_dir, mut doc) = document("name,value\na,1\nb,2\n");
    let path = doc.path().to_path_buf();
    doc.edit_cell(1, 1, "3".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "name,value\na,1\nb,3\n"
    );
    assert_eq!(doc.display_rows(1, 1).unwrap()[0].1[1], "3");
}

#[cfg(unix)]
#[test]
fn save_preserves_existing_destination_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let (dir, mut doc) = document("a,b\nx,1\n");
    let path = dir.path().join("permissions.csv");
    std::fs::write(&path, "a,b\nx,1\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    doc.save(&path).unwrap();

    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o640
    );
}

#[cfg(unix)]
#[test]
fn save_through_symlink_updates_referent_without_replacing_link() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let referent = dir.path().join("actual.csv");
    let link = dir.path().join("linked.csv");
    std::fs::write(&referent, "name,value\na,1\n").unwrap();
    symlink("actual.csv", &link).unwrap();

    let mut doc = CsvDocument::open(&link, None, None).unwrap();
    doc.edit_cell(0, 1, "2".to_string()).unwrap();
    doc.save(&link).unwrap();

    assert!(std::fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        std::fs::read_to_string(&referent).unwrap(),
        "name,value\na,2\n"
    );
    assert_eq!(doc.path(), link.as_path());
}

#[cfg(unix)]
#[test]
fn symlink_retargeted_during_save_aborts_without_touching_either_referent() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    let link = dir.path().join("linked.csv");
    std::fs::write(&first, "name,value\na,1\n").unwrap();
    std::fs::write(&second, "name,value\nb,2\n").unwrap();
    symlink("first.csv", &link).unwrap();
    let mut doc = CsvDocument::open(&link, None, None).unwrap();
    doc.edit_cell(0, 1, "edited".to_string()).unwrap();
    let retargeted = std::cell::Cell::new(false);
    let link_for_progress = link.clone();
    let progress = |_, _| {
        if !retargeted.replace(true) {
            std::fs::remove_file(&link_for_progress).unwrap();
            symlink("second.csv", &link_for_progress).unwrap();
        }
    };

    let error = doc
        .save_cancellable_with_progress(&link, &CancellationToken::new(), &progress)
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::DestinationChanged);
    assert!(error.contains("changed on disk"));
    assert_eq!(
        std::fs::read_to_string(&first).unwrap(),
        "name,value\na,1\n"
    );
    assert_eq!(
        std::fs::read_to_string(&second).unwrap(),
        "name,value\nb,2\n"
    );
    assert!(doc.is_dirty());
}

#[cfg(unix)]
#[test]
fn parent_symlink_retargeted_during_save_is_detected() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let first_dir = dir.path().join("first");
    let second_dir = dir.path().join("second");
    std::fs::create_dir_all(&first_dir).unwrap();
    std::fs::create_dir_all(&second_dir).unwrap();
    let first = first_dir.join("data.csv");
    let second = second_dir.join("data.csv");
    std::fs::write(&first, "name,value\na,1\n").unwrap();
    std::fs::write(&second, "name,value\nb,2\n").unwrap();
    let parent_link = dir.path().join("current");
    symlink("first", &parent_link).unwrap();
    let logical_path = parent_link.join("data.csv");
    let mut doc = CsvDocument::open(&logical_path, None, None).unwrap();
    doc.edit_cell(0, 1, "edited".to_string()).unwrap();
    let retargeted = std::cell::Cell::new(false);
    let link_for_progress = parent_link.clone();
    let progress = |_, _| {
        if !retargeted.replace(true) {
            std::fs::remove_file(&link_for_progress).unwrap();
            symlink("second", &link_for_progress).unwrap();
        }
    };

    assert!(doc
        .save_cancellable_with_progress(&logical_path, &CancellationToken::new(), &progress,)
        .unwrap_err()
        .contains("changed on disk"));
    assert_eq!(
        std::fs::read_to_string(&first).unwrap(),
        "name,value\na,1\n"
    );
    assert_eq!(
        std::fs::read_to_string(&second).unwrap(),
        "name,value\nb,2\n"
    );
}

#[test]
fn failed_save_validation_leaves_destination_and_edits_intact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.csv");
    let original = b"a,b\nx,1\n";
    std::fs::write(&path, original).unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            malformed: Some("repair".to_string()),
            max_field_size: Some(3),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(0, 0, "too-long".to_string()).unwrap();

    assert!(doc.save(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(doc.is_dirty());
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "too-long");
}

#[test]
fn headerless_document_can_save_after_all_rows_are_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("headerless.csv");
    std::fs::write(&path, "1,2\n").unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(false),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.delete_display_row(0).unwrap();

    doc.save(&path).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"");
    assert_eq!(doc.row_count(), 0);
}

#[test]
fn save_validation_preserves_disabled_escape_and_comment_settings() {
    let (dir, mut doc) = document("a,b\nx,1\n");
    doc.edit_cell(0, 0, "#not-a-comment".to_string()).unwrap();
    doc.edit_cell(0, 1, "backslash\\\"quote".to_string())
        .unwrap();
    let output = dir.path().join("syntax.csv");

    doc.save(&output).unwrap();
    assert_eq!(doc.row_count(), 1);
    assert_eq!(
        doc.display_rows(0, 1).unwrap()[0].1,
        vec!["#not-a-comment", "backslash\\\"quote"]
    );
}

#[test]
fn external_change_during_save_is_checked_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.csv");
    std::fs::write(&path, "a,b\nx,1\ny,2\n").unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(0, 1, "edited".to_string()).unwrap();
    let changed = std::cell::Cell::new(false);
    let external_path = path.clone();
    let progress = |_, _| {
        if !changed.replace(true) {
            std::fs::write(&external_path, "a,b\nexternal,8\ny,2\n").unwrap();
        }
    };
    let cancellation = CancellationToken::new();

    let error = doc
        .save_cancellable_with_progress(&path, &cancellation, &progress)
        .unwrap_err();
    assert!(error.contains("changed on disk"));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "a,b\nexternal,8\ny,2\n"
    );
    assert!(doc.is_dirty());
}

#[test]
fn missing_destination_commit_rejects_a_changed_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("new.csv");
    let temporary = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
    std::fs::write(temporary.path(), "a,b\nvalidated,1\n").unwrap();
    let expected = file_fingerprint_cancellable(temporary.path(), &|| false).unwrap();
    let expected_state = capture_open_file_state(temporary.as_file()).unwrap();
    std::fs::write(temporary.path(), "a,b\nchanged,2\n").unwrap();

    let resolved_target = resolve_save_target(&target).unwrap();
    let error = commit_temporary(
        temporary,
        &target,
        &resolved_target,
        DestinationState::Missing,
        expected,
        expected_state,
    )
    .unwrap_err();

    assert!(error.contains("candidate changed"));
    assert!(!target.exists());
}

#[test]
fn concurrent_save_as_destination_change_is_not_overwritten() {
    let (dir, mut doc) = document("a,b\noriginal,1\n");
    doc.edit_cell(0, 1, "edited".to_string()).unwrap();
    let target = dir.path().join("save-as.csv");
    std::fs::write(&target, "a,b\nbefore,2\n").unwrap();
    let changed = std::cell::Cell::new(false);
    let external_target = target.clone();
    let progress = |_, _| {
        if !changed.replace(true) {
            std::fs::write(&external_target, "a,b\nexternal,9\n").unwrap();
        }
    };
    let cancellation = CancellationToken::new();

    let error = doc
        .save_cancellable_with_progress(&target, &cancellation, &progress)
        .unwrap_err();
    assert!(error.contains("changed on disk"));
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "a,b\nexternal,9\n"
    );
}

#[test]
fn explicit_external_overwrite_uses_the_immutable_opened_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("overwrite.csv");
    std::fs::write(&path, "a,b\noriginal,1\nsecond,2\n").unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(0, 1, "edited".to_string()).unwrap();
    std::fs::write(&path, "a,b\nexternal,8\nchanged,9\n").unwrap();
    let cancellation = CancellationToken::new();

    doc.save_cancellable_with_progress_overwrite_external(&path, &cancellation, &|_, _| {})
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "a,b\noriginal,edited\nsecond,2\n"
    );
}

#[test]
fn explicit_external_overwrite_can_restore_a_deleted_source() {
    let (dir, mut doc) = document("a,b\noriginal,1\n");
    let path = dir.path().join("sample.csv");
    doc.edit_cell(0, 1, "restored".to_string()).unwrap();
    std::fs::remove_file(&path).unwrap();
    let cancellation = CancellationToken::new();

    doc.save_cancellable_with_progress_overwrite_external(&path, &cancellation, &|_, _| {})
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "a,b\noriginal,restored\n"
    );
}
