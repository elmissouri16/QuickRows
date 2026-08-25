#[test]
fn serializes_rectangular_cell_ranges_with_edits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cells.csv");
    std::fs::write(&path, "name,left,right\na,1,x\nb,2,y\nc,3,z\n").unwrap();
    let mut doc = CsvDocument::open(
        path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(1, 1, "two, edited".to_string()).unwrap();
    let cancellation = CancellationToken::new();
    let progress = std::cell::Cell::new(0);
    let text = doc
        .serialize_display_cell_range_cancellable_with_progress(
            0,
            2,
            1,
            2,
            &cancellation,
            &|processed, _| progress.set(processed),
        )
        .unwrap();
    assert_eq!(text, "1,x\n\"two, edited\",y\n3,z");
    assert_eq!(progress.get(), 3);
}

#[test]
fn cached_open_and_sort_create_reusable_cache_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cached.csv");
    let cache_root = dir.path().join("cache");
    std::fs::write(&path, "name,value\nb,2\na,1\n").unwrap();
    let overrides = ParseOverrides {
        has_headers: Some(true),
        ..Default::default()
    };
    let sort = SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    };
    let mut document =
        CsvDocument::open_cached(&path, Some(overrides.clone()), None, &cache_root).unwrap();
    document.sort(Some(sort)).unwrap();
    let cache_dir = document.disk_cache_dir.clone().unwrap();
    let cache_key = document.disk_cache_key.unwrap();
    let offsets_path = offsets_cache_path(&cache_dir, cache_key);
    let warnings_path = warnings_cache_path(&cache_dir, cache_key);
    let order_path = order_cache_path(&cache_dir, cache_key, sort.column, true);
    assert!(offsets_path.is_file());
    assert!(warnings_path.is_file());
    assert!(order_path.is_file());

    let order_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&order_path)
        .unwrap();
    let marker = std::time::SystemTime::now() + std::time::Duration::from_secs(60 * 60);
    order_file
        .set_times(std::fs::FileTimes::new().set_modified(marker))
        .unwrap();
    let marked_modified = order_file.metadata().unwrap().modified().unwrap();
    drop(order_file);
    drop(document);

    let progress = std::cell::Cell::new(0);
    let mut reopened = CsvDocument::open_cached(
        &path,
        Some(overrides),
        Some(&|rows| progress.set(rows)),
        &cache_root,
    )
    .unwrap();
    assert_eq!(progress.get(), reopened.row_count());
    reopened.sort(Some(sort)).unwrap();
    assert_eq!(reopened.display_rows(0, 2).unwrap()[0].1[0], "a");
    assert_eq!(
        std::fs::metadata(order_path).unwrap().modified().unwrap(),
        marked_modified
    );
}

#[test]
fn cached_open_does_not_rewrite_valid_offset_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cached.csv");
    let cache_root = dir.path().join("cache");
    std::fs::write(&path, "name,value\nb,2\na,1\n").unwrap();
    let overrides = ParseOverrides {
        has_headers: Some(true),
        ..Default::default()
    };
    let document =
        CsvDocument::open_cached(&path, Some(overrides.clone()), None, &cache_root).unwrap();
    let cache_dir = document.disk_cache_dir.clone().unwrap();
    let cache_key = document.disk_cache_key.unwrap();
    let offsets_path = offsets_cache_path(&cache_dir, cache_key);
    drop(document);

    let offsets = std::fs::OpenOptions::new()
        .write(true)
        .open(&offsets_path)
        .unwrap();
    let marker = std::time::SystemTime::now() + std::time::Duration::from_secs(60 * 60);
    offsets
        .set_times(std::fs::FileTimes::new().set_modified(marker))
        .unwrap();
    let marked_modified = offsets.metadata().unwrap().modified().unwrap();
    drop(offsets);

    let reopened = CsvDocument::open_cached(&path, Some(overrides), None, &cache_root).unwrap();
    assert_eq!(reopened.row_count(), 2);
    assert_eq!(
        std::fs::metadata(offsets_path).unwrap().modified().unwrap(),
        marked_modified
    );
}

#[test]
fn cached_open_uses_raw_hash_when_content_changes_at_the_same_length_and_mtime() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cached.csv");
    let cache_root = dir.path().join("cache");
    let original = "name,value\na,1\nb,2\n";
    let replacement = "name,value\nx,9\ny,8\n";
    assert_eq!(original.len(), replacement.len());
    std::fs::write(&path, original).unwrap();
    let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let overrides = ParseOverrides {
        has_headers: Some(true),
        ..Default::default()
    };
    let original_document =
        CsvDocument::open_cached(&path, Some(overrides.clone()), None, &cache_root).unwrap();
    let original_key = original_document.disk_cache_key.unwrap();
    drop(original_document);

    std::fs::write(&path, replacement).unwrap();
    let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    source
        .set_times(std::fs::FileTimes::new().set_modified(original_modified))
        .unwrap();
    drop(source);
    let replacement_fingerprint = file_fingerprint(&path).unwrap();
    assert_eq!(replacement_fingerprint.len, original_key.len);
    assert_eq!(replacement_fingerprint.modified, original_key.modified);
    assert_ne!(
        replacement_fingerprint.content_hash,
        original_key.content_hash
    );
    let replacement_document =
        CsvDocument::open_cached(&path, Some(overrides), None, &cache_root).unwrap();
    let replacement_key = replacement_document.disk_cache_key.unwrap();

    assert_eq!(replacement_key.len, original_key.len);
    assert_eq!(replacement_key.modified, original_key.modified);
    assert_ne!(replacement_key.content_hash, original_key.content_hash);
    assert_ne!(replacement_key.hash, original_key.hash);
    assert_eq!(
        replacement_document.display_rows(0, 2).unwrap()[0].1,
        vec!["x", "9"]
    );
}

#[test]
fn prepared_unicode_sources_reuse_offset_and_sort_caches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cached-unicode.csv");
    let cache_root = dir.path().join("cache");
    std::fs::write(&path, "# note\nname§value\nb§2\na§1\n").unwrap();
    let overrides = ParseOverrides {
        delimiter: Some("§".to_string()),
        comment: Some("#".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let sort = SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    };
    let mut document =
        CsvDocument::open_cached(&path, Some(overrides.clone()), None, &cache_root).unwrap();
    document.sort(Some(sort)).unwrap();
    let cache_dir = document.disk_cache_dir.clone().unwrap();
    let cache_key = document.disk_cache_key.unwrap();
    let offsets_path = offsets_cache_path(&cache_dir, cache_key);
    let warnings_path = warnings_cache_path(&cache_dir, cache_key);
    let order_path = order_cache_path(&cache_dir, cache_key, sort.column, true);
    assert!(offsets_path.is_file());
    assert!(warnings_path.is_file());
    assert!(order_path.is_file());
    let order_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&order_path)
        .unwrap();
    let marker = std::time::SystemTime::now() + std::time::Duration::from_secs(60 * 60);
    order_file
        .set_times(std::fs::FileTimes::new().set_modified(marker))
        .unwrap();
    let marked_modified = order_file.metadata().unwrap().modified().unwrap();
    drop(order_file);
    drop(document);

    let progress = std::cell::Cell::new(0);
    let mut reopened = CsvDocument::open_cached(
        &path,
        Some(overrides),
        Some(&|rows| progress.set(rows)),
        &cache_root,
    )
    .unwrap();
    assert_eq!(progress.get(), reopened.row_count());
    reopened.sort(Some(sort)).unwrap();
    assert_eq!(reopened.display_rows(0, 2).unwrap()[0].1[0], "a");
    assert_eq!(
        std::fs::metadata(order_path).unwrap().modified().unwrap(),
        marked_modified
    );
}

#[test]
fn streaming_search_reports_rows_and_result_batches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stream.csv");
    std::fs::write(&path, "name,value\na,hit\nb,miss\nc,hit\n").unwrap();
    let doc = CsvDocument::open(
        path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    let streamed = std::cell::RefCell::new(Vec::new());
    let processed = std::cell::Cell::new(0);
    let result = doc
        .search_cancellable_streaming(
            "hit",
            Some(1),
            false,
            true,
            &CancellationToken::new(),
            &|batch, done, _| {
                streamed.borrow_mut().extend_from_slice(batch);
                processed.set(done);
            },
        )
        .unwrap();
    assert_eq!(result, vec![0, 2]);
    assert_eq!(*streamed.borrow(), result);
    assert_eq!(processed.get(), doc.row_count());
}

#[test]
fn inverse_sort_lookup_tracks_source_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inverse.csv");
    std::fs::write(&path, "name,value\nc,3\na,1\nb,2\n").unwrap();
    let mut doc = CsvDocument::open(
        path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    }))
    .unwrap();
    assert_eq!(doc.display_row_for_source(0), Some(2));
    assert_eq!(doc.display_row_for_source(1), Some(0));
    assert_eq!(doc.display_row_for_source(2), Some(1));
    doc.sort(None).unwrap();
    assert_eq!(doc.display_row_for_source(2), Some(2));
}
