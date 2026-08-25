use quickrows_core::{CancellationToken, CsvDocument, ParseOverrides, SortDirection, SortSpec};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const EXPECTED_ROWS: usize = 1_000_000;

fn fixture() -> PathBuf {
    std::env::var_os("QUICKROWS_MILLION_FIXTURE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("test-data/generated/million-rows.csv")
        })
}

fn sample_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn overrides() -> Option<ParseOverrides> {
    Some(ParseOverrides {
        has_headers: Some(true),
        ..Default::default()
    })
}

fn open(source: &Path, cache: Option<&Path>) -> CsvDocument {
    let cancellation = CancellationToken::new();
    let document = match cache {
        Some(cache) => {
            CsvDocument::open_cancellable_cached(source, overrides(), None, &cancellation, cache)
        }
        None => CsvDocument::open_cancellable(source, overrides(), None, &cancellation),
    }
    .unwrap();
    assert_eq!(document.row_count(), EXPECTED_ROWS);
    document
}

fn median(mut timings: Vec<Duration>) -> Duration {
    timings.sort_unstable();
    timings[timings.len() / 2]
}

fn measure(samples: usize, mut operation: impl FnMut()) -> Duration {
    median(
        (0..samples)
            .map(|_| {
                let started = Instant::now();
                operation();
                started.elapsed()
            })
            .collect(),
    )
}

fn measure_timed(samples: usize, mut operation: impl FnMut() -> Duration) -> Duration {
    median((0..samples).map(|_| operation()).collect())
}

fn report(name: &str, samples: usize, elapsed: Duration) {
    println!(
        "{{\"benchmark\":\"{name}\",\"samples\":{samples},\"median_ms\":{:.3}}}",
        elapsed.as_secs_f64() * 1_000.0
    );
}

fn main() {
    let source = fixture();
    assert!(
        source.is_file(),
        "generate {} with scripts/generate_million_csv.py first",
        source.display()
    );
    let samples = sample_count("QUICKROWS_BENCH_SAMPLES", 7);
    let save_samples = sample_count("QUICKROWS_BENCH_SAVE_SAMPLES", 3);

    let uncached_open = measure(samples, || {
        black_box(open(&source, None));
    });
    report("open_uncached", samples, uncached_open);

    let cold_cached_open = measure(samples, || {
        let temp = tempfile::tempdir().unwrap();
        black_box(open(&source, Some(&temp.path().join("cache"))));
    });
    report("open_cache_cold", samples, cold_cached_open);

    let warm_temp = tempfile::tempdir().unwrap();
    let warm_cache = warm_temp.path().join("cache");
    drop(open(&source, Some(&warm_cache)));
    let warm_cached_open = measure(samples, || {
        black_box(open(&source, Some(&warm_cache)));
    });
    report("open_cache_warm", samples, warm_cached_open);

    let sort_elapsed = measure_timed(samples, || {
        let mut document = open(&source, None);
        let started = Instant::now();
        document
            .sort(Some(SortSpec {
                column: 1,
                direction: SortDirection::Descending,
            }))
            .unwrap();
        let elapsed = started.elapsed();
        black_box(document);
        elapsed
    });
    report("sort_clean", samples, sort_elapsed);

    let search_document = open(&source, None);
    let column_search = measure(samples, || {
        let matches = search_document
            .search("quoted value", Some(5), false, false)
            .unwrap();
        assert!(!matches.is_empty());
        black_box(matches);
    });
    report("search_column_clean", samples, column_search);

    let all_column_search = measure(samples, || {
        let matches = search_document
            .search("quoted value", None, false, false)
            .unwrap();
        assert!(!matches.is_empty());
        black_box(matches);
    });
    report("search_all_columns_clean", samples, all_column_search);

    let mut dirty_search_document = open(&source, None);
    dirty_search_document
        .edit_cell(0, 1, "benchmark edit".to_string())
        .unwrap();
    let dirty_column_search = measure(samples, || {
        let matches = dirty_search_document
            .search("quoted value", Some(5), false, false)
            .unwrap();
        assert!(!matches.is_empty());
        black_box(matches);
    });
    report("search_column_dirty", samples, dirty_column_search);

    let save_elapsed = measure_timed(save_samples, || {
        let temp = tempfile::tempdir().unwrap();
        let mut document = open(&source, None);
        document
            .edit_cell(0, 0, "benchmark-saved".to_string())
            .unwrap();
        let output = temp.path().join("saved.csv");
        let started = Instant::now();
        document.save(&output).unwrap();
        let elapsed = started.elapsed();
        assert!(output.metadata().unwrap().len() > 50 * 1024 * 1024);
        black_box(document);
        elapsed
    });
    report("save_one_edit", save_samples, save_elapsed);
}
