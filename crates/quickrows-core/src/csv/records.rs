pub fn settings_cache_hash(settings: &ParseSettings) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    settings.delimiter.hash(&mut hasher);
    settings.quote.hash(&mut hasher);
    settings.escape.hash(&mut hasher);
    settings.comment.hash(&mut hasher);
    settings.excel_sep.hash(&mut hasher);
    settings.source_bom.hash(&mut hasher);
    settings.source_bom_len.hash(&mut hasher);
    settings.line_ending.hash(&mut hasher);
    settings.encoding_label.hash(&mut hasher);
    settings.has_headers.hash(&mut hasher);
    settings.malformed.as_str().hash(&mut hasher);
    settings.max_field_size.hash(&mut hasher);
    settings.max_record_size.hash(&mut hasher);
    hasher.finish()
}

pub fn build_reader<R: Read>(
    reader: R,
    settings: &ParseSettings,
    has_headers: bool,
) -> csv::Reader<R> {
    let mut builder = ReaderBuilder::new();
    debug_assert!(settings.delimiter.is_ascii());
    debug_assert!(settings.quote.is_ascii());
    debug_assert!(settings.escape.is_none_or(|value| value.is_ascii()));
    debug_assert!(settings.comment.is_none_or(|value| value.is_ascii()));
    builder
        .delimiter(settings.delimiter as u8)
        .quote(settings.quote as u8)
        .escape(settings.escape.map(|value| value as u8))
        .comment(settings.comment.map(|value| value as u8))
        .terminator(settings.terminator)
        .has_headers(has_headers)
        .flexible(settings.malformed != MalformedMode::Strict);
    builder.from_reader(reader)
}

fn strip_bom(value: &str) -> &str {
    value.strip_prefix('\u{feff}').unwrap_or(value)
}

fn truncate_to_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = 0;
    for (idx, _) in value.char_indices() {
        if idx > max_bytes {
            break;
        }
        end = idx;
    }
    value[..end].to_string()
}

pub fn decode_record(
    record: &ByteRecord,
    settings: &ParseSettings,
    strip_first_bom: bool,
) -> (Vec<String>, bool) {
    let mut had_errors = false;
    let mut fields = Vec::with_capacity(record.len());
    for (idx, field) in record.iter().enumerate() {
        let (decoded, _, errors) = settings.encoding.decode(field);
        if errors {
            had_errors = true;
        }
        let value = if strip_first_bom && idx == 0 {
            strip_bom(&decoded).to_string()
        } else {
            decoded.into_owned()
        };
        fields.push(value);
    }
    (fields, had_errors)
}

fn enforce_size_limits(
    mut fields: Vec<String>,
    settings: &ParseSettings,
    row_index: Option<u64>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Option<Vec<String>>> {
    let mut total = 0usize;
    let mut truncated = false;
    for field in &mut fields {
        if field.len() > settings.max_field_size {
            match settings.malformed {
                MalformedMode::Strict => {
                    return Err(QuickRowsError::invalid_csv(format!(
                        "CSV error: record {:?} field exceeds max size ({} bytes)",
                        row_index, settings.max_field_size
                    )));
                }
                MalformedMode::Skip => {
                    push_warning(
                        warnings,
                        ParseWarning {
                            record: row_index,
                            line: None,
                            byte: None,
                            field: None,
                            kind: "max-field-size".to_string(),
                            message: format!(
                                "Field exceeds max size ({} bytes)",
                                settings.max_field_size
                            ),
                            expected_len: None,
                            len: Some(field.len() as u64),
                        },
                    );
                    return Ok(None);
                }
                MalformedMode::Repair => {
                    truncated = true;
                    *field = truncate_to_bytes(field, settings.max_field_size);
                }
            }
        }
        total += field.len();
    }

    if total > settings.max_record_size {
        match settings.malformed {
            MalformedMode::Strict => {
                return Err(QuickRowsError::invalid_csv(format!(
                    "CSV error: record {:?} exceeds max size ({} bytes)",
                    row_index, settings.max_record_size
                )));
            }
            MalformedMode::Skip => {
                push_warning(
                    warnings,
                    ParseWarning {
                        record: row_index,
                        line: None,
                        byte: None,
                        field: None,
                        kind: "max-record-size".to_string(),
                        message: format!(
                            "Record exceeds max size ({} bytes)",
                            settings.max_record_size
                        ),
                        expected_len: None,
                        len: Some(total as u64),
                    },
                );
                return Ok(None);
            }
            MalformedMode::Repair => {
                truncated = true;
                let mut overflow = total - settings.max_record_size;
                for field in fields.iter_mut().rev() {
                    if overflow == 0 {
                        break;
                    }
                    let remove = overflow.min(field.len());
                    let keep = field.len() - remove;
                    *field = truncate_to_bytes(field, keep);
                    overflow -= remove;
                }
                total = fields.iter().map(String::len).sum();
                debug_assert!(total <= settings.max_record_size);
            }
        }
    }

    if truncated {
        push_warning(
            warnings,
            ParseWarning {
                record: row_index,
                line: None,
                byte: None,
                field: None,
                kind: "repaired".to_string(),
                message: "Record truncated to fit size limits".to_string(),
                expected_len: None,
                len: None,
            },
        );
    }

    Ok(Some(fields))
}

fn apply_length_policy(
    fields: Vec<String>,
    expected_columns: Option<usize>,
    settings: &ParseSettings,
    row_index: Option<u64>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Option<Vec<String>>> {
    if let Some(expected) = expected_columns
        && fields.len() != expected
    {
        match settings.malformed {
            MalformedMode::Strict => {
                return Err(QuickRowsError::invalid_csv(format!(
                    "CSV error: record {:?} has {} fields, expected {}",
                    row_index,
                    fields.len(),
                    expected
                )));
            }
            MalformedMode::Skip => {
                push_warning(
                    warnings,
                    ParseWarning {
                        record: row_index,
                        line: None,
                        byte: None,
                        field: None,
                        kind: "unequal-lengths".to_string(),
                        message: format!(
                            "Record has {} fields, expected {}",
                            fields.len(),
                            expected
                        ),
                        expected_len: Some(expected as u64),
                        len: Some(fields.len() as u64),
                    },
                );
                return Ok(None);
            }
            MalformedMode::Repair => {
                let mut repaired = fields;
                if repaired.len() < expected {
                    repaired.extend(std::iter::repeat_n(
                        String::new(),
                        expected - repaired.len(),
                    ));
                } else if repaired.len() > expected {
                    repaired.truncate(expected);
                }
                push_warning(
                    warnings,
                    ParseWarning {
                        record: row_index,
                        line: None,
                        byte: None,
                        field: None,
                        kind: "repaired".to_string(),
                        message: format!("Record length adjusted to {} fields", expected),
                        expected_len: Some(expected as u64),
                        len: Some(repaired.len() as u64),
                    },
                );
                return Ok(Some(repaired));
            }
        }
    }
    Ok(Some(fields))
}

pub fn detect_headers_for_settings(
    path: impl AsRef<Path>,
    settings: &ParseSettings,
) -> QuickRowsResult<bool> {
    let file = File::open(path)?;
    let mut reader = build_reader(file, settings, false);
    let mut rows = reader.byte_records();
    let first = rows
        .next()
        .transpose()
        .map_err(map_csv_error)?;
    let second = rows
        .next()
        .transpose()
        .map_err(map_csv_error)?;
    let Some((first, second)) = first.zip(second) else {
        return Ok(true);
    };
    let (first, _) = decode_record(&first, settings, true);
    let (second, _) = decode_record(&second, settings, false);
    Ok(looks_like_header(
        &StringRecord::from(first),
        &StringRecord::from(second),
    ))
}

pub fn get_headers(
    path: impl AsRef<Path>,
    settings: &ParseSettings,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<String>> {
    let file = File::open(path)?;
    let mut rdr = build_reader(file, settings, settings.has_headers);

    if settings.has_headers {
        let headers = rdr
            .byte_headers()
            .inspect_err(|err| {
                push_warning(warnings, warning_from_error(err, None));
            })
            .map_err(map_csv_error)?
            .clone();
        let (decoded, had_errors) = decode_record(&headers, settings, true);
        if had_errors {
            push_warning(
                warnings,
                ParseWarning {
                    record: Some(0),
                    line: None,
                    byte: None,
                    field: None,
                    kind: "utf8".to_string(),
                    message: "Header contains invalid encoding".to_string(),
                    expected_len: None,
                    len: None,
                },
            );
        }
        return Ok(decoded);
    }

    let mut record = ByteRecord::new();
    if rdr
        .read_byte_record(&mut record)
        .inspect_err(|err| {
            push_warning(warnings, warning_from_error(err, None));
        })
        .map_err(map_csv_error)?
    {
        let (decoded, _) = decode_record(&record, settings, true);
        let headers = (0..decoded.len())
            .map(|idx| format!("Column {}", idx + 1))
            .collect::<Vec<_>>();
        return Ok(headers);
    }

    Ok(Vec::new())
}
