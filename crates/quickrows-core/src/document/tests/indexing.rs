#[test]
fn ordinary_sources_are_opened_without_a_full_file_snapshot() {
    let mut contents = String::from("a,b\n");
    for row in 0..256 {
        contents.push_str(&format!("row-{row},value-{row}\n"));
    }
    assert!(contents.len() > 1024);
    let (_dir, doc) = document(&contents);
    assert!(doc._prepared_source.is_none());
    assert_eq!(doc.data_path, doc.path);
    assert!(doc.mmap.is_none());
    assert!(doc
        .path
        .parent()
        .unwrap()
        .read_dir()
        .unwrap()
        .all(|entry| !entry.unwrap().file_name().to_string_lossy().starts_with("quickrows-")));
}

#[test]
fn strict_size_limited_sources_are_opened_without_a_full_file_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("strict.csv");
    std::fs::write(&path, "name,value\nalpha,1\nbeta,2\n").unwrap();

    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            malformed: Some("strict".to_string()),
            max_field_size: Some(1024),
            max_record_size: Some(4096),
            ..Default::default()
        }),
        None,
    )
    .unwrap();

    assert_eq!(doc.data_path, path);
    assert!(doc._prepared_source.is_none());
    assert_eq!(dir.path().read_dir().unwrap().count(), 1);
}

#[test]
fn prepared_source_lives_until_a_background_index_build_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prepared-index.csv");
    std::fs::write(&path, "name§value\nalpha§1\nbeta§2\n").unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            delimiter: Some("§".to_string()),
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    let prepared_path = doc.data_path.clone();
    assert_ne!(prepared_path.parent(), path.parent());
    assert!(path.parent().unwrap().read_dir().unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("quickrows-")
    }));
    let build = doc.prepare_search_index_build();
    drop(doc);
    assert!(prepared_path.exists());
    let index = build.build(&CancellationToken::new(), None).unwrap();
    assert_eq!(index.columns.len(), 2);
    assert!(!prepared_path.exists());
}

#[test]
fn background_index_snapshots_are_rejected_after_edits() {
    let (_dir, mut doc) = document("name,value\na,one\nb,two\n");
    let build = doc.prepare_search_index_build();
    doc.edit_cell(0, 1, "changed".to_string()).unwrap();
    let built = build.build(&CancellationToken::new(), None).unwrap();
    assert!(!doc.install_search_index(built));
    assert!(!doc.has_search_index());
    assert_eq!(
        doc.search("changed", Some(1), false, true).unwrap(),
        vec![0]
    );
}

#[test]
fn background_indexes_are_bound_to_their_originating_document() {
    let (_first_dir, first) = document("name,value\na,one\nb,two\n");
    let build = first.prepare_search_index_build();
    let built = build.build(&CancellationToken::new(), None).unwrap();

    let (_second_dir, mut second) = document("name,value\na,one\nb,two\n");
    assert!(!second.install_search_index(built));
    assert!(!second.has_search_index());
}

#[test]
fn background_indexes_are_rejected_after_save_resets_revision() {
    let (dir, mut doc) = document("name,value\na,one\nb,two\n");
    let build = doc.prepare_search_index_build();
    let output = dir.path().join("saved.csv");
    doc.save(&output).unwrap();

    let built = build.build(&CancellationToken::new(), None).unwrap();
    assert!(!doc.install_search_index(built));
    assert!(!doc.has_search_index());
}

#[test]
fn cancelling_lazy_index_replacement_preserves_installed_index() {
    let (_dir, mut doc) = document("name,value\nalpha,one\nbeta,two\n");
    let expected = doc.search("ALPHA", Some(0), false, true).unwrap();
    doc.ensure_search_index_for_column_cancellable(0, &CancellationToken::new(), None)
        .unwrap();
    assert!(doc.has_search_index_for_column(0));

    let cancellation = CancellationToken::new();
    let cancel_from_progress = cancellation.clone();
    let progress = move |_, _| cancel_from_progress.cancel();
    assert!(doc
        .ensure_search_index_for_column_cancellable(1, &cancellation, Some(&progress))
        .unwrap_err()
        .contains("cancelled"));
    assert!(doc.has_search_index_for_column(0));
    assert!(!doc.has_search_index_for_column(1));
    assert_eq!(doc.search("ALPHA", Some(0), false, true).unwrap(), expected);
}

#[test]
fn background_index_honors_cancellation_from_final_progress() {
    let (_dir, doc) = document("name,value\nalpha,one\nbeta,two\n");
    let build = doc.prepare_search_index_build();
    let cancellation = CancellationToken::new();
    let cancel_from_progress = cancellation.clone();
    let progress = move |_, _| cancel_from_progress.cancel();

    assert!(build
        .build(&cancellation, Some(&progress))
        .unwrap_err()
        .contains("cancelled"));
}

#[test]
fn indexed_search_uses_complete_values_and_queries() {
    let prefix = "x".repeat(300);
    let contents = format!("name,value\n{prefix}suffix,1\n{prefix},2\n");
    let (_dir, mut doc) = document(&contents);
    let query = format!("{}suffix", "x".repeat(300));
    let expected_contains = doc.search("suffix", Some(0), false, false).unwrap();
    let expected_exact = doc.search(&query, Some(0), false, true).unwrap();
    doc.build_search_index().unwrap();
    assert_eq!(
        doc.search("suffix", Some(0), false, false).unwrap(),
        expected_contains
    );
    assert_eq!(
        doc.search(&query, Some(0), false, true).unwrap(),
        expected_exact
    );
    assert_eq!(expected_contains, vec![0]);
    assert_eq!(expected_exact, vec![0]);
}

#[test]
fn dirty_column_search_reconciles_edits_and_deletes() {
    let (_dir, mut doc) =
        document_with_headers("name,value\na,target\nb,other\nc,target\nd,target\n");
    doc.edit_source_cell(0, 1, "away".to_string()).unwrap();
    doc.edit_source_cell(1, 1, "TARGET".to_string()).unwrap();
    doc.delete_display_row(2).unwrap();
    doc.edit_source_cell(3, 0, "unrelated".to_string()).unwrap();

    assert_eq!(
        doc.search("target", Some(1), false, true).unwrap(),
        vec![1, 3]
    );
    assert_eq!(doc.search("target", Some(1), true, true).unwrap(), vec![3]);

    let streamed = std::cell::RefCell::new(Vec::new());
    let progress = |batch: &[usize], _processed, _total| {
        streamed.borrow_mut().extend_from_slice(batch);
    };
    let result = doc
        .search_cancellable_streaming(
            "target",
            Some(1),
            false,
            true,
            &CancellationToken::new(),
            &progress,
        )
        .unwrap();
    assert_eq!(result, vec![1, 3]);
    assert_eq!(*streamed.borrow(), vec![1, 3]);
}

#[test]
fn search_and_duplicates_follow_edits_and_deletions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("edited-query.csv");
    std::fs::write(&path, "name,value\na,one\nb,two\nc,two\n").unwrap();
    let mut doc = CsvDocument::open(
        path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.build_search_index().unwrap();
    doc.edit_cell(0, 1, "two".to_string()).unwrap();
    doc.delete_display_row(1).unwrap();

    assert_eq!(
        doc.search("one", Some(1), false, true).unwrap(),
        Vec::<usize>::new()
    );
    assert_eq!(doc.search("two", Some(1), false, true).unwrap(), vec![0, 2]);
    assert_eq!(doc.find_duplicates(Some(1)).unwrap(), vec![0, 2]);

    doc.delete_display_row(2).unwrap();
    assert!(doc.find_duplicates(Some(1)).unwrap().is_empty());
}

#[test]
fn duplicate_and_search_columns_are_validated() {
    let (_dir, doc) = document("name,value\na,1\n");
    assert!(doc.search("a", Some(2), false, false).is_err());
    assert!(doc.find_duplicates(Some(2)).is_err());
}
