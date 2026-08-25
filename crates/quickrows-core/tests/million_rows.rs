use quickrows_core::{CancellationToken, CsvDocument, ParseOverrides, SortDirection, SortSpec};
use std::path::PathBuf;
use std::time::Instant;

fn fixture() -> PathBuf {
    std::env::var_os("QUICKROWS_MILLION_FIXTURE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("test-data/generated/million-rows.csv")
        })
}

#[test]
#[ignore = "run manually after scripts/generate_million_csv.py"]
fn million_row_open_query_sort_copy_delete_restore_and_save() {
    let source = fixture();
    assert!(source.is_file(), "generate {} first", source.display());
    let temp = tempfile::tempdir().unwrap();
    let cancellation = CancellationToken::new();
    let total_started = Instant::now();
    let open_started = Instant::now();
    let mut document = CsvDocument::open_cancellable_cached(
        &source,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
        &cancellation,
        temp.path().join("cache"),
    )
    .unwrap();
    assert_eq!(document.row_count(), 1_000_000);
    eprintln!("million-row open: {:?}", open_started.elapsed());

    let search_started = Instant::now();
    let matches = document
        .search_cancellable("quoted value", Some(5), false, false, &cancellation)
        .unwrap();
    eprintln!(
        "million-row column search: {:?} ({} matches)",
        search_started.elapsed(),
        matches.len()
    );
    assert!(!matches.is_empty());

    let all_search_started = Instant::now();
    let all_matches = document
        .search_cancellable("quoted value", None, false, false, &cancellation)
        .unwrap();
    eprintln!(
        "million-row all-column search: {:?} ({} matches)",
        all_search_started.elapsed(),
        all_matches.len()
    );
    assert_eq!(all_matches, matches);

    let sort_started = Instant::now();
    document
        .sort_cancellable(
            Some(SortSpec {
                column: 1,
                direction: SortDirection::Descending,
            }),
            &cancellation,
        )
        .unwrap();
    eprintln!("million-row sort: {:?}", sort_started.elapsed());

    let copy_started = Instant::now();
    let copied = document
        .serialize_display_rows_cancellable(&(0..10_000).collect::<Vec<_>>(), &cancellation)
        .unwrap();
    eprintln!(
        "million-row copy 10,000: {:?} ({} bytes)",
        copy_started.elapsed(),
        copied.len()
    );
    assert!(!copied.is_empty());

    let rows = (0..document.row_count()).collect::<Vec<_>>();
    let delete_started = Instant::now();
    assert_eq!(
        document
            .set_display_rows_deleted_cancellable(&rows, true, &cancellation)
            .unwrap(),
        rows.len()
    );
    eprintln!("million-row delete: {:?}", delete_started.elapsed());

    let restore_started = Instant::now();
    assert_eq!(
        document
            .set_display_rows_deleted_cancellable(&rows, false, &cancellation)
            .unwrap(),
        rows.len()
    );
    eprintln!("million-row restore: {:?}", restore_started.elapsed());

    document
        .edit_cell(0, 0, "stress-edited".to_string())
        .unwrap();
    let output = temp.path().join("saved-million.csv");
    let save_started = Instant::now();
    document.save_cancellable(&output, &cancellation).unwrap();
    eprintln!("million-row save: {:?}", save_started.elapsed());
    assert!(output.metadata().unwrap().len() > 50 * 1024 * 1024);
    eprintln!("million-row total: {:?}", total_started.elapsed());
}
