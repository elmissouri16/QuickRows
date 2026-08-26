#[test]
fn cancellable_operations_stop_before_work_starts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel.csv");
    std::fs::write(&path, "name,value\na,1\n").unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(
        CsvDocument::open_cancellable(&path, None, None, &cancellation)
            .err()
            .unwrap()
            .contains("cancelled")
    );
    let cache_root = dir.path().join("cache");
    assert!(CsvDocument::open_cancellable_cached(
        &path,
        None,
        None,
        &cancellation,
        &cache_root,
    )
    .err()
    .unwrap()
    .contains("cancelled"));
    assert!(!cache_root.join("csv-index-cache").exists());

    let active_cancellation = CancellationToken::new();
    let cancel_from_progress = active_cancellation.clone();
    let cancel_at_first_row = move |_| cancel_from_progress.cancel();
    assert!(CsvDocument::open_cancellable(
        &path,
        None,
        Some(&cancel_at_first_row),
        &active_cancellation,
    )
    .err()
    .unwrap()
    .contains("cancelled"));

    let (doc_dir, mut doc) = document("name,value\na,1\n");
    assert!(doc
        .search_cancellable("a", Some(0), false, false, &cancellation)
        .unwrap_err()
        .contains("cancelled"));
    assert!(doc
        .build_search_index_cancellable(&cancellation)
        .unwrap_err()
        .contains("cancelled"));
    assert!(doc
        .find_duplicates_cancellable(None, &cancellation)
        .unwrap_err()
        .contains("cancelled"));
    assert!(doc
        .sort_cancellable(
            Some(SortSpec {
                column: 0,
                direction: SortDirection::Ascending,
            }),
            &cancellation,
        )
        .unwrap_err()
        .contains("cancelled"));
    assert!(doc
        .serialize_display_rows_cancellable(&[0], &cancellation)
        .unwrap_err()
        .contains("cancelled"));
    let output = doc_dir.path().join("cancelled-save.csv");
    assert!(doc
        .save_cancellable(&output, &cancellation)
        .unwrap_err()
        .contains("cancelled"));
    assert!(!output.exists());
}

#[test]
fn opens_utf16le_with_bom() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("utf16.csv");
    let mut bytes = vec![0xff, 0xfe];
    for unit in "name,value\r\ncafé,2\r\n".encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    std::fs::write(&path, bytes).unwrap();
    let doc = CsvDocument::open(path, None, None).unwrap();
    assert_eq!(doc.metadata().effective.encoding, "UTF-16LE");
    assert_eq!(doc.metadata().headers, vec!["name", "value"]);
    assert_eq!(
        doc.row_count(),
        1,
        "prepared source: {:?}",
        std::fs::read_to_string(&doc.data_path)
    );
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["café", "2"]);
}

#[test]
fn bomless_utf16_uses_explicit_endianness_for_dialect_detection() {
    for (encoding, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("bomless-{encoding}.csv"));
        let mut bytes = Vec::new();
        for unit in "name;value\r\nalpha;1\r\n".encode_utf16() {
            let encoded = if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            bytes.extend_from_slice(&encoded);
        }
        std::fs::write(&path, bytes).unwrap();
        let overrides = ParseOverrides {
            encoding: Some(encoding.to_string()),
            ..Default::default()
        };
        let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        assert_eq!(doc.metadata().effective.delimiter, ";");
        assert_eq!(doc.metadata().effective.line_ending, "crlf");
        assert_eq!(doc.metadata().headers, vec!["name", "value"]);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
    }
}

#[test]
fn utf16le_and_utf16be_round_trip_through_save() {
    for (label, bom, little_endian) in [
        ("utf-16le", [0xff, 0xfe], true),
        ("utf-16be", [0xfe, 0xff], false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("{label}.csv"));
        let mut bytes = bom.to_vec();
        for unit in "name,note\r\ncafé,\"line 1\r\nline 2\"\r\n".encode_utf16() {
            let encoded = if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            bytes.extend_from_slice(&encoded);
        }
        std::fs::write(&path, bytes).unwrap();

        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert_eq!(doc.row_count(), 1);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "café");
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "line 1\r\nline 2");
        doc.edit_cell(0, 0, "thé".to_string()).unwrap();
        doc.save(&path).unwrap();

        let saved = std::fs::read(&path).unwrap();
        assert_eq!(&saved[..2], &bom);
        let units = saved[2..]
            .chunks_exact(2)
            .map(|pair| {
                if little_endian {
                    u16::from_le_bytes([pair[0], pair[1]])
                } else {
                    u16::from_be_bytes([pair[0], pair[1]])
                }
            })
            .collect::<Vec<_>>();
        let decoded = String::from_utf16(&units).unwrap();
        assert_eq!(decoded, "name,note\r\nthé,\"line 1\r\nline 2\"\r\n");
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "thé");
    }
}

#[test]
fn malformed_utf16_obeys_strict_skip_and_repair_for_both_endiannesses() {
    for (label, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("malformed-{label}.csv"));
        let mut bytes = utf16_bytes("name,value\ngood,1\nbad,", little_endian, true);
        let malformed_unit = if little_endian {
            0xd800u16.to_le_bytes()
        } else {
            0xd800u16.to_be_bytes()
        };
        bytes.extend_from_slice(&malformed_unit);
        bytes.extend_from_slice(&utf16_bytes("\nafter,3\n", little_endian, false));
        std::fs::write(&path, bytes).unwrap();

        let overrides = |mode: &str| ParseOverrides {
            encoding: Some(label.to_string()),
            has_headers: Some(true),
            malformed: Some(mode.to_string()),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());

        let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
        assert_eq!(skipped.row_count(), 2);
        assert_eq!(skipped.display_rows(0, 2).unwrap()[0].1, vec!["good", "1"]);
        assert_eq!(skipped.display_rows(0, 2).unwrap()[1].1, vec!["after", "3"]);
        assert!(skipped
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "encoding"));

        let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 3);
        assert_eq!(repaired.display_rows(1, 1).unwrap()[0].1, vec!["bad", "�"]);
        assert!(repaired
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "encoding"));
    }
}

#[test]
fn odd_trailing_utf16_bytes_obey_all_malformed_policies() {
    for (label, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("odd-byte-{label}.csv"));
        let mut bytes = utf16_bytes("name,value\ngood,1\nbad,", little_endian, true);
        bytes.push(0x61);
        std::fs::write(&path, bytes).unwrap();
        let overrides = |mode: &str| ParseOverrides {
            encoding: Some(label.to_string()),
            has_headers: Some(true),
            malformed: Some(mode.to_string()),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());
        let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
        assert_eq!(skipped.row_count(), 1);
        assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["good", "1"]);
        let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 2);
        assert_eq!(repaired.display_rows(1, 1).unwrap()[0].1, vec!["bad", "�"]);
    }
}

#[test]
fn utf16_decoder_preserves_a_surrogate_pair_split_across_stream_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("split-surrogate.csv");
    let prefix = "name,value\nrow,";
    let prefix_bytes = prefix.encode_utf16().count() * 2;
    let filler_units = (64 * 1024 - 2 - prefix_bytes) / 2;
    let text = format!("{prefix}{}😀\n", "x".repeat(filler_units));
    let bytes = utf16_bytes(&text, true, true);
    std::fs::write(&path, bytes).unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            encoding: Some("utf-16le".to_string()),
            has_headers: Some(true),
            malformed: Some("strict".to_string()),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    let value = &doc.display_rows(0, 1).unwrap()[0].1[1];
    assert!(value.ends_with('😀'));
    assert_eq!(doc.row_count(), 1);
}

#[test]
fn source_preparation_reports_progress_and_honors_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel-utf16.csv");
    let text = format!("name,value\n{}\n", "row,value\n".repeat(20_000));
    std::fs::write(&path, utf16_bytes(&text, true, true)).unwrap();
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let progress = move |bytes: usize| {
        if bytes >= 64 * 1024 {
            cancel.cancel();
        }
    };
    let error = CsvDocument::open_cancellable(
        &path,
        Some(ParseOverrides {
            encoding: Some("utf-16le".to_string()),
            has_headers: Some(true),
            ..Default::default()
        }),
        Some(&progress),
        &cancellation,
    )
    .err()
    .unwrap();
    assert!(error.contains("cancelled"));
}

#[test]
fn unicode_delimiter_quote_and_escape_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unicode-dialect.csv");
    std::fs::write(&path, "name§note\nalpha§«one§two«\nbeta§«say ※«hello※««\n").unwrap();
    let overrides = ParseOverrides {
        delimiter: Some("§".to_string()),
        quote: Some("«".to_string()),
        escape: Some("※".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    assert_eq!(doc.row_count(), 2);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "one§two");
    assert_eq!(doc.display_rows(1, 1).unwrap()[0].1[1], "say «hello«");
    doc.edit_cell(0, 1, "edited § «value«".to_string()).unwrap();
    doc.save(&path).unwrap();
    let saved = std::fs::read_to_string(&path).unwrap();
    assert!(saved.contains("alpha§«edited § ※«value※««"));
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "edited § «value«");
}

#[test]
fn excel_sep_directive_is_detected_skipped_and_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("excel.csv");
    std::fs::write(&path, "sep=;\r\nname;value\r\nalpha;1\r\n").unwrap();
    let mut doc = CsvDocument::open(&path, None, None).unwrap();
    assert!(doc.metadata().effective.excel_sep);
    assert_eq!(doc.metadata().effective.delimiter, ";");
    assert_eq!(doc.metadata().headers, vec!["name", "value"]);
    assert_eq!(doc.row_count(), 1);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
    doc.save(&path).unwrap();
    assert!(std::fs::read_to_string(&path)
        .unwrap()
        .starts_with("sep=;\r\n"));
}

#[test]
fn comment_prefixed_first_fields_are_quoted_on_save() {
    for comment in ["#", "※"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comment-values.csv");
        std::fs::write(
            &path,
            format!("name,value\n\"{comment}literal\",1\nnormal,2\n"),
        )
        .unwrap();
        let overrides = ParseOverrides {
            comment: Some(comment.to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        doc.edit_cell(1, 1, "3".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(doc.row_count(), 2);
        assert_eq!(
            doc.display_rows(0, 1).unwrap()[0].1[0],
            format!("{comment}literal")
        );
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(saved.contains(&format!("\"{comment}literal\",1")));
    }
}

#[test]
fn comment_prefixed_values_round_trip_in_utf16() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("comment-utf16.csv");
    std::fs::write(
        &path,
        utf16_bytes("name,value\r\n\"#literal\",1\r\n", true, true),
    )
    .unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            encoding: Some("utf-16le".to_string()),
            comment: Some("#".to_string()),
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(0, 1, "2".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(doc.row_count(), 1);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["#literal", "2"]);
}

#[test]
fn configured_comments_are_excluded_from_headers_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("comments.csv");
    std::fs::write(
        &path,
        "# generated file\nname,value\nalpha,1\n# middle comment,ignored\nbeta,2\n",
    )
    .unwrap();
    let overrides = ParseOverrides {
        comment: Some("#".to_string()),
        ..Default::default()
    };
    let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    assert_eq!(doc.metadata().headers, vec!["name", "value"]);
    assert_eq!(doc.row_count(), 2);
    assert_eq!(doc.display_rows(1, 1).unwrap()[0].1, vec!["beta", "2"]);
}

#[test]
fn comments_are_preserved_in_their_record_positions_when_saving() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("preserved-comments.csv");
    let contents = concat!(
        "# before header\n",
        "name,value\n",
        "alpha,1\n",
        "# before beta\n",
        "beta,2\n",
        "# trailing\n",
    );
    std::fs::write(&path, contents).unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            comment: Some("#".to_string()),
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    doc.edit_cell(0, 1, "3".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        concat!(
            "# before header\n",
            "name,value\n",
            "alpha,3\n",
            "# before beta\n",
            "beta,2\n",
            "# trailing\n",
        )
    );
}

#[test]
fn standards_compliant_large_fields_have_no_default_rejection_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large-field.csv");
    let value = "x".repeat(300 * 1024);
    std::fs::write(&path, format!("name,value\nalpha,{value}\n")).unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert_eq!(doc.row_count(), 1);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1].len(), value.len());
}

#[test]
fn standards_compliant_large_records_have_no_default_rejection_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large-record.csv");
    let headers = (0..11).map(|index| format!("c{index}")).collect::<Vec<_>>();
    let fields = std::iter::once("row".to_string())
        .chain((0..10).map(|_| "x".repeat(220 * 1024)))
        .collect::<Vec<_>>();
    std::fs::write(
        &path,
        format!("{}\n{}\n", headers.join(","), fields.join(",")),
    )
    .unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert_eq!(doc.row_count(), 1);
    assert!(
        doc.display_rows(0, 1).unwrap()[0]
            .1
            .iter()
            .map(String::len)
            .sum::<usize>()
            > 2 * 1024 * 1024
    );
}

#[test]
fn utf8_bom_is_preserved_and_does_not_affect_no_header_search() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bom.csv");
    std::fs::write(&path, b"\xef\xbb\xbfalpha,1\nalpha,2\n").unwrap();
    let overrides = ParseOverrides {
        has_headers: Some(false),
        ..Default::default()
    };
    let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    assert_eq!(
        doc.search("alpha", Some(0), true, true).unwrap(),
        vec![0, 1]
    );
    assert_eq!(doc.find_duplicates(Some(0)).unwrap(), vec![0, 1]);
    doc.edit_cell(0, 1, "3".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert!(std::fs::read(&path).unwrap().starts_with(b"\xef\xbb\xbf"));
}

#[test]
fn a_bom_is_removed_by_detected_length_even_when_encoding_is_overridden() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("forced-encoding.csv");
    std::fs::write(&path, b"\xef\xbb\xbfname,value\nalpha,1\n").unwrap();
    let mut doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            encoding: Some("windows-1252".to_string()),
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert_eq!(doc.metadata().headers, vec!["name", "value"]);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
    doc.edit_cell(0, 1, "2".to_string()).unwrap();
    doc.save(&path).unwrap();
    assert!(!std::fs::read(&path).unwrap().starts_with(b"\xef\xbb\xbf"));
}

#[test]
fn bomless_utf16_stays_bomless_after_save() {
    for (label, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("bomless-save-{label}.csv"));
        std::fs::write(
            &path,
            utf16_bytes("name,value\nalpha,1\n", little_endian, false),
        )
        .unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                encoding: Some(label.to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 1, "2".to_string()).unwrap();
        doc.save(&path).unwrap();
        let saved = std::fs::read(&path).unwrap();
        assert!(!saved.starts_with(b"\xff\xfe"));
        assert!(!saved.starts_with(b"\xfe\xff"));
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "2"]);
    }
}

#[test]
fn utf16_excel_sep_and_unicode_syntax_work_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("utf16-unicode-sep.csv");
    let text = "sep=§\r\nname§note\r\nalpha§«one§two«\r\n";
    std::fs::write(&path, utf16_bytes(text, true, true)).unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            encoding: Some("utf-16le".to_string()),
            delimiter: Some("§".to_string()),
            quote: Some("«".to_string()),
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert!(doc.metadata().effective.excel_sep);
    assert_eq!(doc.metadata().headers, vec!["name", "note"]);
    assert_eq!(
        doc.display_rows(0, 1).unwrap()[0].1,
        vec!["alpha", "one§two"]
    );
}

#[test]
fn rfc7111_fragments_resolve_against_header_and_data_rows() {
    let (_dir, doc) = document("name,value\nalpha,1\nbeta,2\n");
    let rows = "row=1;3".parse::<CsvFragment>().unwrap();
    assert_eq!(
        doc.resolve_fragment(&rows),
        vec![
            ResolvedFragmentRegion::Rows(0..=0),
            ResolvedFragmentRegion::Rows(2..=2),
        ]
    );
    let cells = "cell=2,1-3,2".parse::<CsvFragment>().unwrap();
    assert_eq!(
        doc.resolve_fragment(&cells),
        vec![ResolvedFragmentRegion::Cells {
            rows: 1..=2,
            columns: 0..=1,
        }]
    );
}

#[test]
fn invalid_parse_overrides_are_reported_instead_of_silently_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid-overrides.csv");
    std::fs::write(&path, "name,value\nalpha,1\n").unwrap();
    for overrides in [
        ParseOverrides {
            delimiter: Some("two".to_string()),
            ..Default::default()
        },
        ParseOverrides {
            encoding: Some("made-up-encoding".to_string()),
            ..Default::default()
        },
        ParseOverrides {
            malformed: Some("maybe".to_string()),
            ..Default::default()
        },
        ParseOverrides {
            line_ending: Some("vertical".to_string()),
            ..Default::default()
        },
        ParseOverrides {
            delimiter: Some("|".to_string()),
            quote: Some("|".to_string()),
            ..Default::default()
        },
        ParseOverrides {
            delimiter: Some("|".to_string()),
            comment: Some("|".to_string()),
            ..Default::default()
        },
    ] {
        assert!(CsvDocument::open(&path, Some(overrides), None).is_err());
    }
}

#[test]
fn strict_mode_rejects_invalid_quote_grammar() {
    for contents in [
        "name,value\nalpha,ba\"d\n",
        "name,value\nalpha,\"unclosed\n",
        "name,value\nalpha,\"closed\"tail\n",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.csv");
        std::fs::write(&path, contents).unwrap();
        let overrides = ParseOverrides {
            malformed: Some("strict".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(overrides), None).is_err());
    }
}

#[test]
fn empty_physical_records_follow_the_selected_malformed_policy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blank-record.csv");
    std::fs::write(&path, "name,value\n\nalpha,1\n").unwrap();
    let overrides = |mode: &str| ParseOverrides {
        malformed: Some(mode.to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());
    let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
    assert_eq!(skipped.row_count(), 1);
    assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
    let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
    assert_eq!(repaired.row_count(), 2);
    assert_eq!(repaired.display_rows(0, 1).unwrap()[0].1, vec!["", ""]);
    assert_eq!(
        repaired.display_rows(1, 1).unwrap()[0].1,
        vec!["alpha", "1"]
    );
}

#[test]
fn malformed_width_modes_are_strict_skip_and_repair() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("widths.csv");
    std::fs::write(&path, "name,value\nalpha,1\nmissing\nbeta,2,extra\n").unwrap();

    let strict = ParseOverrides {
        malformed: Some("strict".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    assert!(CsvDocument::open(&path, Some(strict), None).is_err());

    let skip = ParseOverrides {
        malformed: Some("skip".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let skipped = CsvDocument::open(&path, Some(skip), None).unwrap();
    assert_eq!(skipped.row_count(), 1);
    assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);

    let repair = ParseOverrides {
        malformed: Some("repair".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let repaired = CsvDocument::open(&path, Some(repair), None).unwrap();
    assert_eq!(repaired.row_count(), 3);
    assert_eq!(
        repaired.display_rows(1, 1).unwrap()[0].1,
        vec!["missing", ""]
    );
    assert_eq!(repaired.display_rows(2, 1).unwrap()[0].1, vec!["beta", "2"]);
}

#[test]
fn invalid_encoding_follows_strict_skip_and_repair_modes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid-utf8.csv");
    std::fs::write(&path, b"name,value\nalpha,\xff\nbeta,ok\n").unwrap();
    let settings = |mode: &str| ParseOverrides {
        encoding: Some("utf-8".to_string()),
        malformed: Some(mode.to_string()),
        has_headers: Some(true),
        ..Default::default()
    };

    assert!(CsvDocument::open(&path, Some(settings("strict")), None).is_err());
    let skipped = CsvDocument::open(&path, Some(settings("skip")), None).unwrap();
    assert_eq!(skipped.row_count(), 1);
    assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["beta", "ok"]);
    assert!(skipped
        .metadata()
        .warnings
        .iter()
        .any(|warning| warning.kind == "encoding"));

    let repaired = CsvDocument::open(&path, Some(settings("repair")), None).unwrap();
    assert_eq!(repaired.row_count(), 2);
    assert_eq!(repaired.display_rows(0, 1).unwrap()[0].1[1], "�");
    assert!(repaired
        .metadata()
        .warnings
        .iter()
        .any(|warning| warning.kind == "encoding"));
}

#[test]
fn warning_collection_is_bounded_for_highly_malformed_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("many-warnings.csv");
    let mut contents = String::from("name,value\n");
    for index in 0..(MAX_WARNING_COUNT + 75) {
        contents.push_str(&format!("missing-{index}\n"));
    }
    std::fs::write(&path, contents).unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            malformed: Some("skip".to_string()),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert_eq!(doc.row_count(), 0);
    assert_eq!(doc.metadata().warnings.len(), MAX_WARNING_COUNT);
}

#[test]
fn spaces_and_nul_bytes_are_preserved_as_field_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("significant-content.csv");
    std::fs::write(&path, b"name,value\n  alpha  ,\0inside\n").unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    assert_eq!(
        doc.display_rows(0, 1).unwrap()[0].1,
        vec!["  alpha  ", "\0inside"]
    );
}

#[test]
fn record_size_repair_preserves_the_rectangular_row_shape() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("record-limit.csv");
    std::fs::write(&path, "name,left,right\nrow,1234,5678\n").unwrap();
    let settings = |mode: &str| ParseOverrides {
        malformed: Some(mode.to_string()),
        has_headers: Some(true),
        max_record_size: Some(7),
        ..Default::default()
    };
    assert!(CsvDocument::open(&path, Some(settings("strict")), None).is_err());
    assert_eq!(
        CsvDocument::open(&path, Some(settings("skip")), None)
            .unwrap()
            .row_count(),
        0
    );
    let repaired = CsvDocument::open(&path, Some(settings("repair")), None).unwrap();
    assert_eq!(repaired.row_count(), 1);
    assert_eq!(
        repaired.display_rows(0, 1).unwrap()[0].1,
        vec!["row", "1234", ""]
    );
    assert_eq!(repaired.display_rows(0, 1).unwrap()[0].1.len(), 3);
    assert!(repaired
        .metadata()
        .warnings
        .iter()
        .any(|warning| warning.kind == "repaired"));
}

#[test]
fn repaired_field_truncation_does_not_consume_the_record_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("combined-limits.csv");
    std::fs::write(&path, "aaaa,bb\n").unwrap();
    let doc = CsvDocument::open(
        &path,
        Some(ParseOverrides {
            has_headers: Some(false),
            malformed: Some("repair".to_string()),
            max_field_size: Some(2),
            max_record_size: Some(4),
            ..Default::default()
        }),
        None,
    )
    .unwrap();

    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["aa", "bb"]);
}

#[test]
fn explicit_size_limits_apply_in_all_malformed_modes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("limits.csv");
    std::fs::write(&path, "name,value\nalpha,abcd\n").unwrap();

    let settings = |mode: &str| ParseOverrides {
        malformed: Some(mode.to_string()),
        has_headers: Some(true),
        max_field_size: Some(3),
        max_record_size: Some(32),
        ..Default::default()
    };
    let strict_error = CsvDocument::open(&path, Some(settings("strict")), None)
        .err()
        .expect("strict size limits should reject the document");
    assert_eq!(strict_error.kind(), crate::ErrorKind::InvalidCsv);
    let skipped = CsvDocument::open(&path, Some(settings("skip")), None).unwrap();
    assert_eq!(skipped.row_count(), 0);
    assert_eq!(
        skipped
            .metadata()
            .warnings
            .iter()
            .filter(|warning| warning.kind == "max-field-size")
            .count(),
        1
    );
    let repaired = CsvDocument::open(&path, Some(settings("repair")), None).unwrap();
    assert_eq!(repaired.row_count(), 1);
    assert_eq!(
        repaired.display_rows(0, 1).unwrap()[0].1,
        vec!["alp", "abc"]
    );
}

#[test]
fn malformed_unicode_quote_grammar_obeys_all_policies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("malformed-unicode.csv");
    std::fs::write(&path, "name§value\ngood§«one«\nbad§«unclosed\n").unwrap();
    let overrides = |mode: &str| ParseOverrides {
        delimiter: Some("§".to_string()),
        quote: Some("«".to_string()),
        has_headers: Some(true),
        malformed: Some(mode.to_string()),
        ..Default::default()
    };
    assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());

    let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
    assert_eq!(skipped.row_count(), 1);
    assert_eq!(
        skipped.display_rows(0, 1).unwrap()[0].1,
        vec!["good", "one"]
    );
    assert!(skipped
        .metadata()
        .warnings
        .iter()
        .any(|warning| warning.kind == "malformed-quote"));

    let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
    assert_eq!(repaired.row_count(), 2);
    assert_eq!(repaired.display_rows(1, 1).unwrap()[0].1[0], "bad");
    assert!(repaired.display_rows(1, 1).unwrap()[0].1[1].contains("unclosed"));
    assert!(repaired
        .metadata()
        .warnings
        .iter()
        .any(|warning| warning.kind == "malformed-quote"));
}

#[test]
fn unicode_comment_character_is_supported() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unicode-comments.csv");
    std::fs::write(
        &path,
        "※ generated\nname,value\nalpha,1\n※ ignored\nbeta,2\n",
    )
    .unwrap();
    let overrides = ParseOverrides {
        comment: Some("※".to_string()),
        has_headers: Some(true),
        ..Default::default()
    };
    let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    assert_eq!(doc.row_count(), 2);
    assert_eq!(doc.display_rows(1, 1).unwrap()[0].1, vec!["beta", "2"]);
}

#[test]
fn disabling_excel_sep_treats_directive_as_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disabled-sep.csv");
    std::fs::write(&path, "sep=;\nname;value\nalpha;1\n").unwrap();
    let overrides = ParseOverrides {
        excel_sep: Some(false),
        delimiter: Some("semicolon".to_string()),
        has_headers: Some(false),
        malformed: Some("repair".to_string()),
        ..Default::default()
    };
    let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
    assert!(!doc.metadata().effective.excel_sep);
    assert_eq!(doc.row_count(), 3);
    assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "sep=");
}


#[test]
fn missing_source_has_io_error_kind() {
    let dir = tempfile::tempdir().unwrap();
    let error = CsvDocument::open(dir.path().join("missing.csv"), None, None)
        .err()
        .expect("missing source should fail");
    assert_eq!(error.kind(), crate::ErrorKind::Io);
}
