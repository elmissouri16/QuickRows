#[test]
fn no_op_edits_and_restored_deletes_are_clean() {
    let (_dir, mut doc) = document("name,value\na,1\n");
    doc.edit_cell(0, 1, "1".to_string()).unwrap();
    assert!(!doc.is_dirty());
    doc.delete_display_row(0).unwrap();
    assert!(doc.is_dirty());
    doc.restore_display_row(0).unwrap();
    assert!(!doc.is_dirty());
}

#[test]
fn opens_reads_searches_and_sorts() {
    let (_dir, mut doc) = document("name,value\nbeta,2\nalpha,1\n");
    assert_eq!(doc.row_count(), 2);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "beta");
    assert_eq!(doc.search("alpha", None, false, false).unwrap(), vec![1]);
    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    }))
    .unwrap();
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "alpha");
}

#[test]
fn source_cell_edits_keep_their_identity_after_sorting() {
    let (_dir, mut doc) = document("name,value\nbeta,2\nalpha,1\n");
    let beta_source_row = doc.source_row_for_display(0).unwrap();
    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    }))
    .unwrap();
    doc.edit_source_cell(beta_source_row, 1, "9".to_string())
        .unwrap();
    let beta_display_row = doc.display_row_for_source(beta_source_row).unwrap();
    assert_eq!(doc.display_rows(beta_display_row, 1).unwrap()[0].1[1], "9");
}

#[test]
fn search_index_matches_case_insensitive_column_searches() {
    let (_dir, mut doc) = document("name,value\nAlpha,first\nbeta,second\nalphabet,third\n");
    let contains_before = doc.search("ALPHA", Some(0), false, false).unwrap();
    let exact_before = doc.search("ALPHA", Some(0), false, true).unwrap();
    doc.build_search_index().unwrap();
    assert!(doc.has_search_index());
    assert_eq!(
        doc.search("ALPHA", Some(0), false, false).unwrap(),
        contains_before
    );
    assert_eq!(
        doc.search("ALPHA", Some(0), false, true).unwrap(),
        exact_before
    );
    doc.clear_search_index();
    assert!(!doc.has_search_index());
}

#[test]
fn on_demand_search_index_retains_only_the_selected_column() {
    let (_dir, mut doc) = document("name,value\nAlpha,first\nbeta,second\nalphabet,first\n");
    let cancellation = CancellationToken::new();
    let name_matches = doc.search("ALPHA", Some(0), false, false).unwrap();
    let value_matches = doc.search("FIRST", Some(1), false, true).unwrap();
    let indexed_rows = std::cell::Cell::new(0);
    let report_progress = |processed, _| indexed_rows.set(processed);

    doc.ensure_search_index_for_column_cancellable(0, &cancellation, Some(&report_progress))
        .unwrap();
    assert_eq!(indexed_rows.get(), doc.row_count());
    assert!(doc.has_search_index_for_column(0));
    assert!(!doc.has_search_index_for_column(1));
    assert_eq!(
        doc.search("ALPHA", Some(0), false, false).unwrap(),
        name_matches
    );

    doc.ensure_search_index_for_column_cancellable(1, &cancellation, None)
        .unwrap();
    assert!(!doc.has_search_index_for_column(0));
    assert!(doc.has_search_index_for_column(1));
    assert_eq!(
        doc.search("FIRST", Some(1), false, true).unwrap(),
        value_matches
    );
}

#[test]
fn dirty_on_demand_column_index_matches_dirty_scan() {
    let (_dir, mut doc) =
        document_with_headers("name,value\na,Alpha\nb,beta\nc,alphabet\nd,Alpha\n");
    doc.edit_source_cell(0, 0, "unrelated".to_string()).unwrap();
    doc.edit_source_cell(1, 1, "Alpha".to_string()).unwrap();
    doc.edit_source_cell(2, 1, "changed".to_string()).unwrap();
    doc.delete_display_row(3).unwrap();
    let contains = doc.search("ALPHA", Some(1), false, false).unwrap();
    let exact = doc.search("ALPHA", Some(1), false, true).unwrap();

    doc.ensure_search_index_for_column_cancellable(1, &CancellationToken::new(), None)
        .unwrap();

    assert!(doc.has_search_index_for_column(1));
    assert!(!doc.has_search_index_for_column(0));
    assert_eq!(
        doc.search("ALPHA", Some(1), false, false).unwrap(),
        contains
    );
    assert_eq!(doc.search("ALPHA", Some(1), false, true).unwrap(), exact);
    assert_eq!(contains, vec![0, 1]);
    assert_eq!(exact, vec![0, 1]);
}

#[test]
fn compact_postings_preserve_single_and_duplicate_rows() {
    let mut postings = RowPostings::One(3);
    assert_eq!(postings.as_slice(), &[3]);
    postings.push(8);
    postings.push(13);
    assert_eq!(postings.as_slice(), &[3, 8, 13]);
}

#[test]
fn sorting_uses_values_beyond_the_first_256_bytes() {
    let prefix = "x".repeat(256);
    let csv = format!("name,value\n{prefix}z,2\n{prefix}a,1\n");
    let (_dir, mut doc) = document(&csv);
    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    }))
    .unwrap();
    assert!(doc.display_rows(0, 1).unwrap()[0].1[0].ends_with('a'));
}

#[test]
fn sort_uses_current_projected_column_and_source_ties() {
    let (_dir, mut doc) = document_with_headers("key,note\nz,zero\na,one\na,two\nb,three\n");
    doc.edit_source_cell(0, 0, "a".to_string()).unwrap();
    doc.edit_source_cell(1, 1, "unrelated".to_string()).unwrap();
    doc.delete_display_row(2).unwrap();

    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Ascending,
    }))
    .unwrap();
    assert_eq!(doc.sorted_order.as_deref(), Some(&[0, 1, 2, 3][..]));

    doc.sort(Some(SortSpec {
        column: 0,
        direction: SortDirection::Descending,
    }))
    .unwrap();
    assert_eq!(doc.sorted_order.as_deref(), Some(&[3, 0, 1, 2][..]));
    assert!(doc.edits.is_deleted(2));
}

#[test]
fn bounded_sort_checks_cancellation_before_all_comparisons_finish() {
    const TEST_CHUNK_SIZE: usize = 1_024;
    let values = (0..TEST_CHUNK_SIZE * 64)
        .map(|row| {
            let key = row
                .wrapping_mul(2_654_435_761usize)
                .rotate_left((row % 31) as u32);
            format!("{key:016x}")
        })
        .collect::<Vec<_>>();

    let full_comparisons = std::sync::atomic::AtomicUsize::new(0);
    sort_projected_order(&values, true, TEST_CHUNK_SIZE, &|| Ok(()), &|| {
        full_comparisons.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    let full_comparisons = full_comparisons.load(Ordering::Relaxed);

    let cancelled_comparisons = std::sync::atomic::AtomicUsize::new(0);
    let cancellation_checks = std::sync::atomic::AtomicUsize::new(0);
    let check_cancellation = || {
        cancellation_checks.fetch_add(1, Ordering::Relaxed);
        if cancelled_comparisons.load(Ordering::Relaxed) >= 10_000 {
            Err(QuickRowsError::cancelled())
        } else {
            Ok(())
        }
    };
    let result = sort_projected_order(&values, true, TEST_CHUNK_SIZE, &check_cancellation, &|| {
        cancelled_comparisons.fetch_add(1, Ordering::Relaxed);
    });
    let cancelled_comparisons = cancelled_comparisons.load(Ordering::Relaxed);

    let error = result.unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Cancelled);
    assert!(error.contains("cancelled"));
    assert!(cancellation_checks.load(Ordering::Relaxed) > 1);
    assert!(cancelled_comparisons >= 10_000);
    assert!(cancelled_comparisons < full_comparisons);
}

#[test]
fn cancellable_sort_merges_chunks_with_source_row_ties() {
    let row_count = SORT_MERGE_CHUNK_SIZE + 37;
    let mut contents = String::with_capacity(row_count * 16);
    contents.push_str("key,row\n");
    for row in 0..row_count {
        let key = row.wrapping_mul(7_919) % 997;
        contents.push_str(&format!("{key:04},{row}\n"));
    }
    let (_dir, mut doc) = document_with_headers(&contents);
    doc.sort_cancellable(
        Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }),
        &CancellationToken::new(),
    )
    .unwrap();

    let mut expected = (0..row_count).collect::<Vec<_>>();
    expected.sort_unstable_by_key(|row| (row.wrapping_mul(7_919) % 997, *row));
    assert_eq!(doc.sorted_order.as_deref(), Some(expected.as_slice()));
}

#[test]
fn cancellable_sort_stops_during_bounded_sort_work() {
    let row_count = SORT_MERGE_CHUNK_SIZE * 4 + 17;
    let mut contents = String::with_capacity(row_count * 24);
    contents.push_str("key,row\n");
    for row in 0..row_count {
        let key = row
            .wrapping_mul(2_654_435_761)
            .rotate_left((row % 31) as u32);
        contents.push_str(&format!("{key:016x},{row}\n"));
    }
    let (_dir, mut doc) = document_with_headers(&contents);
    let cancellation = CancellationToken::new();
    let cancel_sort = cancellation.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let canceller = std::thread::spawn(move || {
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));
        cancel_sort.cancel();
    });
    let signalled = std::cell::Cell::new(false);
    let progress = |processed, total| {
        if processed == total && !signalled.replace(true) {
            started_tx.send(()).unwrap();
        }
    };

    let result = doc.sort_cancellable_with_progress(
        Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }),
        &cancellation,
        &progress,
    );
    canceller.join().unwrap();

    assert!(result.unwrap_err().contains("cancelled"));
    assert!(doc.sorted_order.is_none());
}

#[test]
fn invalid_sort_column_has_out_of_range_kind() {
    let (_dir, mut doc) = document_with_headers(
        "name,value
a,1
",
    );
    let error = doc
        .sort(Some(SortSpec {
            column: 99,
            direction: SortDirection::Ascending,
        }))
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::OutOfRange);
    assert_eq!(error.to_string(), "Sort column is out of range");
}

#[test]
fn operation_context_keeps_cancellation_and_progress_together() {
    let cancellation = CancellationToken::new();
    let progress = |_: usize, _: usize| {};
    let context = OperationContext::new(Some(&cancellation), Some(&progress));
    assert!(context.check().is_ok());
    assert!(context.progress.is_some());

    cancellation.cancel();
    assert_eq!(
        context.check().unwrap_err().kind(),
        crate::ErrorKind::Cancelled
    );
}
