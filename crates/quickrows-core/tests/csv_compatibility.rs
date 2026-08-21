use quickrows_core::{CsvDocument, ParseOverrides};

#[test]
fn rfc4180_records_quotes_commas_and_crlf_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("rfc4180.csv");
    let saved = dir.path().join("saved.csv");
    let contents = concat!(
        "Name,Note,Extra\r\n",
        "Alice,\"comma, inside\",\r\n",
        "Bob,\"line 1\r\nline 2\",\"said \"\"hello\"\"\"\r\n",
    );
    std::fs::write(&source, contents).unwrap();

    let mut document = CsvDocument::open(
        &source,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert_eq!(document.metadata().headers, vec!["Name", "Note", "Extra"]);
    assert_eq!(document.row_count(), 2);
    assert_eq!(
        document.display_rows(0, 2).unwrap()[0].1,
        vec!["Alice", "comma, inside", ""]
    );
    assert_eq!(
        document.display_rows(0, 2).unwrap()[1].1,
        vec!["Bob", "line 1\r\nline 2", "said \"hello\""]
    );

    document.save(&saved).unwrap();
    assert_eq!(std::fs::read_to_string(saved).unwrap(), contents);
}

#[test]
fn optional_header_and_missing_final_terminator_are_supported() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("no-header.csv");
    std::fs::write(&source, "alpha,1\nbeta,2").unwrap();
    let overrides = ParseOverrides {
        has_headers: Some(false),
        ..Default::default()
    };

    let document = CsvDocument::open(&source, Some(overrides), None).unwrap();
    assert_eq!(document.metadata().headers, vec!["Column 1", "Column 2"]);
    assert_eq!(document.row_count(), 2);
    assert_eq!(document.display_rows(1, 1).unwrap()[0].1, vec!["beta", "2"]);
}

#[test]
fn dialect_matrix_covers_line_endings_quotes_and_delimiters() {
    let cases = [
        (
            "name;note\r\nalpha;'one;two'\r\n",
            "semicolon",
            "single",
            "crlf",
            "one;two",
        ),
        (
            "name\tnote\nalpha\t\"one\ttwo\"\n",
            "tab",
            "double",
            "lf",
            "one\ttwo",
        ),
        (
            "name|note\ralpha|\"line\ninside\"\r",
            "pipe",
            "double",
            "cr",
            "line\ninside",
        ),
    ];

    for (contents, delimiter, quote, ending, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("dialect.csv");
        std::fs::write(&source, contents).unwrap();
        let overrides = ParseOverrides {
            delimiter: Some(delimiter.to_string()),
            quote: Some(quote.to_string()),
            line_ending: Some(ending.to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut document = CsvDocument::open(&source, Some(overrides), None).unwrap();
        assert_eq!(document.metadata().headers, vec!["name", "note"]);
        assert_eq!(document.row_count(), 1, "{contents:?}");
        assert_eq!(
            document.display_rows(0, 1).unwrap()[0].1,
            vec!["alpha", expected]
        );
        let saved = dir.path().join("dialect-saved.csv");
        document.save(&saved).unwrap();
        assert_eq!(std::fs::read_to_string(saved).unwrap(), contents);
    }
}
