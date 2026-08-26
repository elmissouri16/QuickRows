use super::*;
use std::io::{SeekFrom, Write};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct SeekCountingReader {
    inner: Cursor<Vec<u8>>,
    seeks: Arc<AtomicUsize>,
}

impl Read for SeekCountingReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Seek for SeekCountingReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.seeks.fetch_add(1, Ordering::Relaxed);
        self.inner.seek(position)
    }
}

fn write_temp_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    file.write_all(contents.as_bytes()).expect("write csv");
    file.flush().expect("flush csv");
    file
}

#[test]
fn whitespace_syntax_characters_are_not_trimmed_away() {
    assert_eq!(normalize_delimiter(" "), Some(' '));
    assert_eq!(normalize_quote(" "), Some(' '));
    assert_eq!(normalize_quote("\t"), Some('\t'));
    assert_eq!(normalize_escape(" "), Some(Some(' ')));
    assert_eq!(normalize_comment("\t"), Some(Some('\t')));
}

#[test]
fn apply_parse_overrides_respects_inputs() {
    let detected = DetectedSettings {
        delimiter: ',',
        quote: '"',
        escape: None,
        comment: None,
        excel_sep: false,
        source_bom: false,
        source_bom_len: 0,
        line_ending: "lf".to_string(),
        encoding: encoding_rs::UTF_8,
        encoding_label: "utf-8".to_string(),
        has_headers: true,
    };
    let overrides = ParseOverrides {
        delimiter: Some("tab".to_string()),
        quote: Some("single".to_string()),
        escape: Some("backslash".to_string()),
        comment: Some("hash".to_string()),
        excel_sep: Some(false),
        line_ending: Some("crlf".to_string()),
        encoding: Some("utf8".to_string()),
        has_headers: Some(false),
        malformed: Some("repair".to_string()),
        max_field_size: Some(10),
        max_record_size: Some(20),
    };

    let settings = apply_parse_overrides(&detected, Some(overrides));
    assert_eq!(settings.delimiter, '\t');
    assert_eq!(settings.quote, '\'');
    assert_eq!(settings.escape, Some('\\'));
    assert_eq!(settings.comment, Some('#'));
    assert!(!settings.excel_sep);
    assert!(matches!(settings.terminator, Terminator::CRLF));
    assert_eq!(settings.line_ending, "crlf".to_string());
    assert_eq!(settings.encoding_label, "utf-8".to_string());
    assert!(!settings.has_headers);
    assert_eq!(settings.malformed, MalformedMode::Repair);
    assert_eq!(settings.max_field_size, 10);
    assert_eq!(settings.max_record_size, 20);
}

#[test]
fn parse_info_from_settings_round_trip() {
    let settings = default_parse_settings();
    let info = parse_info_from_settings(&settings);
    assert_eq!(info.delimiter, ",");
    assert_eq!(info.quote, "\"");
    assert_eq!(info.escape, None);
    assert_eq!(info.comment, None);
    assert!(!info.excel_sep);
    assert_eq!(info.line_ending, "auto");
    assert_eq!(info.encoding, "utf-8");
    assert!(info.has_headers);
    assert_eq!(info.malformed, "skip");
    assert_eq!(info.max_field_size, usize::MAX);
    assert_eq!(info.max_record_size, usize::MAX);
}

#[test]
fn detects_common_custom_and_unicode_delimiters() {
    for (contents, delimiter, line_ending) in [
        ("name,value\nalpha,1\n", ',', "lf"),
        ("name;value\r\nalpha;1\r\n", ';', "crlf"),
        ("name\tvalue\ralpha\t1\r", '\t', "cr"),
        ("name:value\nalpha:1\n", ':', "lf"),
        ("name§value\nalpha§1\n", '§', "lf"),
        ("one|two|three", '|', "crlf"),
    ] {
        let file = write_temp_csv(contents);
        let detected = detect_parse_settings(file.path()).unwrap();
        assert_eq!(detected.delimiter, delimiter, "{contents:?}");
        assert_eq!(detected.line_ending, line_ending, "{contents:?}");
    }
}

#[test]
fn detection_does_not_treat_ordinary_punctuation_as_delimiters() {
    for contents in [
        "date\n2024-01-01\n2024-01-02\n",
        "amount\n12.50\n10.25\n",
        "url\nhttps://example.com/a\nhttps://example.com/b\n",
        "path\n/usr/local/bin\n/usr/local/share\n",
        "text\nhello! hello!\ngoodbye? goodbye?\n",
    ] {
        let file = write_temp_csv(contents);
        let detected = detect_parse_settings(file.path()).unwrap();
        assert_eq!(detected.delimiter, ',', "{contents:?}");
    }
}

#[test]
fn all_text_data_is_not_silently_consumed_as_a_header() {
    let file = write_temp_csv("alice,paris\nbob,london\n");
    let detected = detect_parse_settings(file.path()).unwrap();
    assert!(!detected.has_headers);

    let single = write_temp_csv("alice,paris\n");
    let detected = detect_parse_settings(single.path()).unwrap();
    assert!(!detected.has_headers);
}

#[test]
fn detects_single_quoted_multiline_records_without_counting_embedded_newlines() {
    let file = write_temp_csv("name;note\r\nalpha;'line 1\nline 2'\r\n");
    let detected = detect_parse_settings(file.path()).unwrap();
    assert_eq!(detected.quote, '\'');
    assert_eq!(detected.delimiter, ';');
    assert_eq!(detected.line_ending, "crlf");
}

#[test]
fn apostrophes_in_text_are_not_inferred_as_csv_quotes() {
    let file = write_temp_csv("name,note\nalice,don't guess single quotes\n");
    let detected = detect_parse_settings(file.path()).unwrap();
    assert_eq!(detected.quote, '"');
    assert_eq!(detected.delimiter, ',');
}

#[test]
fn delimiter_detection_handles_quoted_unicode_delimiters_and_truncated_samples() {
    let unicode = write_temp_csv("name§note\nalpha§\"one§two\"\n");
    let detected = detect_parse_settings(unicode.path()).unwrap();
    assert_eq!(detected.delimiter, '§');

    let mut contents = String::from("name,note\nalpha,\"");
    contents.push_str(&"x".repeat(SAMPLE_SIZE + 512));
    contents.push_str("\"\n");
    let truncated = write_temp_csv(&contents);
    let detected = detect_parse_settings(truncated.path()).unwrap();
    assert_eq!(detected.delimiter, ',');
    assert_eq!(detected.line_ending, "lf");
}

#[test]
fn detects_excel_separator_without_treating_it_as_a_header() {
    let file = write_temp_csv("sep=;\r\nname;value\r\nalpha;1\r\n");
    let detected = detect_parse_settings(file.path()).unwrap();
    assert_eq!(detected.delimiter, ';');
    assert!(detected.excel_sep);
    assert!(detected.has_headers);
}

#[test]
fn detects_utf16_endianness_and_decoded_line_endings() {
    for (bom, little_endian, label) in [
        ([0xff, 0xfe], true, "UTF-16LE"),
        ([0xfe, 0xff], false, "UTF-16BE"),
    ] {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&bom).unwrap();
        for unit in "name;value\r\nalpha;1\r\n".encode_utf16() {
            let bytes = if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            file.write_all(&bytes).unwrap();
        }
        file.flush().unwrap();
        let detected = detect_parse_settings(file.path()).unwrap();
        assert_eq!(detected.encoding_label, label);
        assert_eq!(detected.delimiter, ';');
        assert_eq!(detected.line_ending, "crlf");
        assert!(detected.source_bom);
    }
}

#[test]
fn skip_mode_omits_encoding_invalid_records_in_direct_reads() {
    let mut settings = default_parse_settings();
    settings.has_headers = false;
    settings.malformed = MalformedMode::Skip;
    let mut warnings = Vec::new();

    let rows = read_chunk_mmap(
        b"good\n\xff\nlast\n",
        0,
        10,
        &settings,
        Some(1),
        &mut warnings,
    )
    .unwrap();

    assert_eq!(
        rows,
        vec![vec!["good".to_string()], vec!["last".to_string()]]
    );
    assert!(warnings.iter().any(|warning| warning.kind == "utf8"));
}

#[test]
fn chunk_ranges_saturate_instead_of_overflowing() {
    let mut settings = default_parse_settings();
    settings.has_headers = false;
    let mut warnings = Vec::new();

    let rows =
        read_chunk_mmap(b"value\n", usize::MAX, 2, &settings, Some(1), &mut warnings).unwrap();

    assert!(rows.is_empty());
}

#[test]
fn finite_size_limits_validate_without_copying_the_source() {
    let mut source = tempfile::NamedTempFile::new().unwrap();
    source.write_all(b"name,value\nrow,").unwrap();
    source.write_all(&vec![b'x'; 2 * 1024 * 1024]).unwrap();
    source.write_all(b"\n").unwrap();
    source.flush().unwrap();

    let mut settings = default_parse_settings();
    settings.has_headers = true;
    settings.malformed = MalformedMode::Skip;
    settings.max_field_size = 32;
    settings.max_record_size = 64;
    let prepared = prepare_csv_source(source.path(), &settings).unwrap();

    assert!(prepared.temporary.is_none());
    assert_eq!(prepared.path, source.path());
    assert!(std::fs::metadata(&prepared.path).unwrap().len() > 2 * 1024 * 1024);
    assert!(
        prepared
            .warnings
            .iter()
            .any(|warning| warning.kind == "max-field-size")
    );
}

#[test]
fn build_offsets_and_read_chunk_with_offsets() {
    let file = write_temp_csv("col1,col2\r\nalpha,1\r\nbeta,2\r\ngamma,3\r\n");
    let settings = default_parse_settings();
    let mut warnings = Vec::new();
    let offsets = build_row_offsets(file.path(), &settings, Some(2), &mut warnings, None)
        .expect("build offsets");
    assert_eq!(offsets.len(), 3);
    assert!(warnings.is_empty());

    let mut chunk_warnings = Vec::new();
    let rows = read_chunk_with_offsets(
        file.path(),
        &offsets,
        1,
        1,
        &settings,
        Some(2),
        &mut chunk_warnings,
    )
    .expect("read chunk");
    assert_eq!(rows, vec![vec!["beta".to_string(), "2".to_string()]]);
}

#[test]
fn contiguous_index_reads_seek_once() {
    let seeks = Arc::new(AtomicUsize::new(0));
    let reader = SeekCountingReader {
        inner: Cursor::new(b"a,b\nfirst,1\nsecond,2\nthird,3\n".to_vec()),
        seeks: seeks.clone(),
    };
    let settings = default_parse_settings();
    let csv_reader = build_reader(reader, &settings, false);
    let mut warnings = Vec::new();

    let rows = read_rows_by_index_from_reader(
        csv_reader,
        &[4, 12, 21],
        &[0, 1, 2],
        &settings,
        Some(2),
        &mut warnings,
    )
    .expect("read contiguous rows");

    assert_eq!(rows[0][0], "first");
    assert_eq!(rows[1][0], "second");
    assert_eq!(rows[2][0], "third");
    assert!(seeks.load(Ordering::Relaxed) <= 1);
}

#[test]
fn offset_reads_seek_past_skipped_malformed_rows() {
    let mut file = write_temp_csv("a,b\nfirst,1\nbad,row,extra\nsecond,2\n");
    let settings = default_parse_settings();
    let mut warnings = Vec::new();
    let offsets = build_row_offsets(file.path(), &settings, Some(2), &mut warnings, None).unwrap();
    assert_eq!(offsets.len(), 2);
    let rows = read_chunk_with_offsets(
        file.path(),
        &offsets,
        0,
        2,
        &settings,
        Some(2),
        &mut warnings,
    )
    .unwrap();
    assert_eq!(rows[0][0], "first");
    assert_eq!(rows[1][0], "second");
    let matches = search_range_with_offsets(
        file.path(),
        &offsets,
        0,
        offsets.len(),
        None,
        "SECOND",
        false,
        false,
        &settings,
    )
    .unwrap();
    assert_eq!(matches, vec![1]);
    file.flush().unwrap();
}

#[test]
fn search_range_whole_word_and_contains() {
    let file = write_temp_csv("col\r\nalpha\r\nalphabet\r\nbeta\r\n");
    let settings = default_parse_settings();
    let mut warnings = Vec::new();
    let offsets = build_row_offsets(file.path(), &settings, Some(1), &mut warnings, None)
        .expect("build offsets");

    let whole_word = search_range_with_offsets(
        file.path(),
        &offsets,
        0,
        offsets.len(),
        Some(0),
        "alpha",
        false,
        true,
        &settings,
    )
    .expect("search whole word");
    assert_eq!(whole_word, vec![0]);

    let contains = search_range_with_offsets(
        file.path(),
        &offsets,
        0,
        offsets.len(),
        Some(0),
        "alpha",
        false,
        false,
        &settings,
    )
    .expect("search contains");
    assert_eq!(contains, vec![0, 1]);
}

#[test]
fn decode_record_strips_bom() {
    let data = "\u{feff}Name,Value\r\nAlice,1\r\n";
    let mut settings = default_parse_settings();
    settings.has_headers = false;
    let mut rdr = build_reader(data.as_bytes(), &settings, false);
    let mut record = csv::ByteRecord::new();
    assert!(rdr.read_byte_record(&mut record).expect("read record"));
    let (decoded, had_errors) = decode_record(&record, &settings, true);
    assert!(!had_errors);
    assert_eq!(decoded[0], "Name");
}

#[test]
fn saved_preparation_indexes_directly_readable_output_without_copying() {
    let file = write_temp_csv("name,note\r\nalpha,\"line one\nline two\"\r\nbeta,plain\r\n");
    let mut settings = default_parse_settings();
    settings.malformed = MalformedMode::Strict;
    let saved = prepare_saved_csv_source_cancellable(
        file.path(),
        &settings,
        &["name".to_string(), "note".to_string()],
        2,
        &|| false,
    )
    .expect("prepare saved source");
    let mut warnings = Vec::new();
    let scanned_offsets = build_row_offsets(
        &saved.prepared.path,
        &saved.prepared.settings,
        Some(2),
        &mut warnings,
        None,
    )
    .expect("scan canonical offsets");

    assert_eq!(saved.headers, vec!["name", "note"]);
    assert_eq!(saved.offsets, scanned_offsets);
    let raw = std::fs::read(file.path()).unwrap();
    assert_eq!(saved.raw_len, raw.len() as u64);
    assert_eq!(saved.raw_content_hash, *blake3::hash(&raw).as_bytes());
    assert_eq!(saved.prepared.path, file.path());
    assert!(saved.prepared.temporary.is_none());
    assert_eq!(std::fs::read(&saved.prepared.path).unwrap(), raw);
    assert!(warnings.is_empty());
}

#[test]
fn column_range_projection_is_aligned_and_matches_mmap() {
    let file = write_temp_csv("id,value,tail\r\n1,\"two,2\",x\r\n2,\"line\nbreak\",\r\n");
    let settings = default_parse_settings();
    let mut warnings = Vec::new();
    let offsets = build_row_offsets(file.path(), &settings, Some(3), &mut warnings, None)
        .expect("build offsets");
    let expected = vec![Some("two,2".to_string()), Some("line\nbreak".to_string())];

    let projected = read_column_range_with_offsets(
        file.path(),
        &offsets,
        0,
        offsets.len(),
        1,
        &settings,
        Some(3),
        &mut Vec::new(),
    )
    .expect("project file column");
    let data = std::fs::read(file.path()).expect("read file");
    let mmap_projected = read_column_range_with_offsets_mmap(
        &data,
        &offsets,
        0,
        offsets.len(),
        1,
        &settings,
        Some(3),
        &mut Vec::new(),
    )
    .expect("project mmap column");

    assert_eq!(projected, expected);
    assert_eq!(mmap_projected, expected);
    assert_eq!(
        read_column_range_with_offsets(
            file.path(),
            &offsets,
            1,
            2,
            2,
            &settings,
            Some(3),
            &mut Vec::new(),
        )
        .unwrap(),
        vec![Some(String::new())]
    );
}

#[test]
fn column_projection_applies_record_repair_before_projection() {
    let file = write_temp_csv("abc,def\n");
    let mut settings = default_parse_settings();
    settings.has_headers = false;
    settings.malformed = MalformedMode::Repair;
    settings.max_record_size = 2;
    let mut warnings = Vec::new();
    let offsets = build_row_offsets(file.path(), &settings, Some(2), &mut warnings, None)
        .expect("build offsets");

    let projected = read_column_range_with_offsets(
        file.path(),
        &offsets,
        0,
        1,
        0,
        &settings,
        Some(2),
        &mut Vec::new(),
    )
    .expect("project repaired column");
    let full = read_chunk_with_offsets(
        file.path(),
        &offsets,
        0,
        1,
        &settings,
        Some(2),
        &mut Vec::new(),
    )
    .expect("read repaired row");

    assert_eq!(projected, vec![Some("ab".to_string())]);
    assert_eq!(full[0][0], "ab");
    assert_eq!(full[0][1], "");
}

#[test]
fn find_duplicates_hashed_matches_rows() {
    let file = write_temp_csv("id,name\r\n1,Alice\r\n2,Bob\r\n1,Alice\r\n3,Charlie\r\n2,Bob\r\n");
    let settings = default_parse_settings();
    let mut warnings = Vec::new();
    let offsets = build_row_offsets(file.path(), &settings, Some(2), &mut warnings, None)
        .expect("build offsets");

    let duplicates =
        find_duplicates_hashed(file.path(), &offsets, &settings, None).expect("find duplicates");
    assert_eq!(duplicates, vec![0, 1, 2, 4]);

    let data = std::fs::read(file.path()).expect("read file");
    let mmap_duplicates =
        find_duplicates_hashed_mmap(&data, &offsets, &settings, None).expect("find mmap");
    assert_eq!(mmap_duplicates, vec![0, 1, 2, 4]);
}

#[test]
fn override_validation_classifies_invalid_settings() {
    let error = validate_parse_overrides(&ParseOverrides {
        encoding: Some("not-an-encoding".to_string()),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::InvalidSettings);
}

#[test]
fn override_validation_uses_detected_values_for_conflicts() {
    let detected = ParseInfo {
        delimiter: ",".to_string(),
        quote: "\"".to_string(),
        escape: None,
        comment: None,
        excel_sep: false,
        line_ending: "lf".to_string(),
        encoding: "utf-8".to_string(),
        has_headers: true,
        malformed: "strict".to_string(),
        max_field_size: 0,
        max_record_size: 0,
    };
    let error = validate_parse_overrides_for_info(
        &ParseOverrides {
            quote: Some(",".to_string()),
            ..Default::default()
        },
        Some(&detected),
    )
    .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::InvalidSettings);
    assert!(error.contains("delimiter and quote"));
}

#[test]
fn cancellable_offset_build_reports_typed_cancellation() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "a,b\n1,2\n").unwrap();
    let settings = default_parse_settings();
    let error =
        build_row_offsets_cancellable(file.path(), &settings, None, &mut Vec::new(), None, &|| {
            true
        })
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Cancelled);
}

#[test]
fn boxed_csv_io_failures_remain_typed_as_io() {
    let csv_error = csv::Error::from(std::io::Error::other("read failed"));
    let error = map_boxed_csv_error(Box::new(csv_error));
    assert_eq!(error.kind(), crate::ErrorKind::Io);

    let boxed_io: Box<dyn std::error::Error> = Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "non-CSV invalid data",
    ));
    let error = crate::QuickRowsError::from(boxed_io);
    assert_eq!(error.kind(), crate::ErrorKind::Io);
}

#[test]
fn strict_direct_helpers_classify_size_failures_as_invalid_csv() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "oversized\n").unwrap();
    let mut settings = default_parse_settings();
    settings.has_headers = false;
    settings.malformed = MalformedMode::Strict;
    settings.max_field_size = 3;

    let offset_error =
        build_row_offsets(file.path(), &settings, Some(1), &mut Vec::new(), None).unwrap_err();
    assert_eq!(offset_error.kind(), crate::ErrorKind::InvalidCsv);

    let read_error =
        read_chunk(file.path(), 0, 1, &settings, Some(1), &mut Vec::new()).unwrap_err();
    assert_eq!(read_error.kind(), crate::ErrorKind::InvalidCsv);
}

#[test]
fn buffered_and_mmap_duplicate_queries_share_verification_semantics() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "a,b\nx,1\ny,2\nx,1\nx,3\ny,2\n").unwrap();
    let settings = default_parse_settings();
    let offsets = build_row_offsets(file.path(), &settings, None, &mut Vec::new(), None).unwrap();
    let data = std::fs::read(file.path()).unwrap();

    let buffered = find_duplicates_hashed(file.path(), &offsets, &settings, None).unwrap();
    let mapped = find_duplicates_hashed_mmap(&data, &offsets, &settings, None).unwrap();
    assert_eq!(buffered, vec![0, 1, 2, 4]);
    assert_eq!(mapped, buffered);

    let buffered_column =
        find_duplicates_hashed(file.path(), &offsets, &settings, Some(0)).unwrap();
    let mapped_column = find_duplicates_hashed_mmap(&data, &offsets, &settings, Some(0)).unwrap();
    assert_eq!(buffered_column, vec![0, 1, 2, 3, 4]);
    assert_eq!(mapped_column, buffered_column);
}
