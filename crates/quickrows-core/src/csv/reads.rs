fn read_chunk_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    let mut record = ByteRecord::new();
    let mut row_index: u64 = 0;
    let mut kept_index: usize = 0;
    let target_end = start.saturating_add(count);

    loop {
        match rdr.read_byte_record(&mut record) {
            Ok(false) => break,
            Ok(true) => {
                let strip_bom = !settings.has_headers && row_index == 0;
                let (decoded, had_errors) = decode_record(&record, settings, strip_bom);
                if had_errors {
                    push_warning(
                        warnings,
                        ParseWarning {
                            record: Some(row_index),
                            line: None,
                            byte: None,
                            field: None,
                            kind: "utf8".to_string(),
                            message: "Record contains invalid encoding".to_string(),
                            expected_len: None,
                            len: None,
                        },
                    );
                    if settings.malformed == MalformedMode::Strict {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("CSV error: record {} has invalid encoding", row_index),
                        )));
                    }
                    if settings.malformed == MalformedMode::Skip {
                        row_index += 1;
                        continue;
                    }
                }

                let decoded = match apply_length_policy(
                    decoded,
                    expected_columns,
                    settings,
                    Some(row_index),
                    warnings,
                )? {
                    Some(row) => row,
                    None => {
                        row_index += 1;
                        continue;
                    }
                };

                let decoded =
                    match enforce_size_limits(decoded, settings, Some(row_index), warnings)? {
                        Some(row) => row,
                        None => {
                            row_index += 1;
                            continue;
                        }
                    };

                if kept_index >= start && kept_index < target_end {
                    rows.push(decoded);
                }
                kept_index += 1;
                if kept_index >= target_end {
                    break;
                }
                row_index += 1;
            }
            Err(err) => {
                let warning = warning_from_error(&err, Some(row_index));
                push_warning(warnings, warning);
                if settings.malformed == MalformedMode::Strict {
                    return Err(Box::new(err));
                }
                row_index += 1;
            }
        }
    }

    Ok(rows)
}

fn read_chunk_with_offsets_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    if start >= offsets.len() {
        return Ok(Vec::new());
    }

    let end = start.saturating_add(count).min(offsets.len());
    let mut record = ByteRecord::new();
    let mut rows = Vec::with_capacity(end - start);
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;

    for (row_index, &row_offset) in offsets.iter().enumerate().take(end).skip(start) {
        if rdr.position().byte() != row_offset {
            let mut position = Position::new();
            position.set_byte(row_offset);
            rdr.seek(position)?;
        }
        if !rdr.read_byte_record(&mut record)? {
            break;
        }
        let strip_bom = !settings.has_headers && row_index == 0;
        let (decoded, had_errors) = decode_record(&record, settings, strip_bom);
        if had_errors {
            push_warning(
                warnings,
                ParseWarning {
                    record: Some(row_index as u64),
                    line: None,
                    byte: None,
                    field: None,
                    kind: "utf8".to_string(),
                    message: "Record contains invalid encoding".to_string(),
                    expected_len: None,
                    len: None,
                },
            );
            if settings.malformed == MalformedMode::Strict {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("CSV error: record {} has invalid encoding", row_index),
                )));
            }
            if settings.malformed == MalformedMode::Skip {
                continue;
            }
        }

        let decoded = match apply_length_policy(
            decoded,
            expected_columns,
            settings,
            Some(row_index as u64),
            warnings,
        )? {
            Some(row) => row,
            None => continue,
        };

        let decoded =
            match enforce_size_limits(decoded, settings, Some(row_index as u64), warnings)? {
                Some(row) => row,
                None => continue,
            };

        rows.push(decoded);
    }

    Ok(rows)
}

fn project_column_from_record(
    record: &ByteRecord,
    row_index: usize,
    column_idx: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let valid_width = expected_columns.is_none_or(|expected| record.len() == expected);
    let simple_utf8 = settings.encoding == encoding_rs::UTF_8
        && settings.max_field_size == usize::MAX
        && settings.max_record_size == usize::MAX
        && valid_width
        && record
            .iter()
            .all(|field| std::str::from_utf8(field).is_ok());
    if simple_utf8 {
        let Some(field) = record.get(column_idx) else {
            return Ok(None);
        };
        let value = std::str::from_utf8(field)?;
        return Ok(Some(
            if !settings.has_headers && row_index == 0 && column_idx == 0 {
                strip_bom(value)
            } else {
                value
            }
            .to_string(),
        ));
    }

    let strip_bom = !settings.has_headers && row_index == 0;
    let (decoded, had_errors) = decode_record(record, settings, strip_bom);
    if had_errors {
        push_warning(
            warnings,
            ParseWarning {
                record: Some(row_index as u64),
                line: None,
                byte: None,
                field: None,
                kind: "utf8".to_string(),
                message: "Record contains invalid encoding".to_string(),
                expected_len: None,
                len: None,
            },
        );
        if settings.malformed == MalformedMode::Strict {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("CSV error: record {} has invalid encoding", row_index),
            )));
        }
        if settings.malformed == MalformedMode::Skip {
            return Ok(None);
        }
    }

    let decoded = match apply_length_policy(
        decoded,
        expected_columns,
        settings,
        Some(row_index as u64),
        warnings,
    )? {
        Some(row) => row,
        None => return Ok(None),
    };
    let decoded = match enforce_size_limits(decoded, settings, Some(row_index as u64), warnings)? {
        Some(row) => row,
        None => return Ok(None),
    };
    Ok(decoded.get(column_idx).cloned())
}

#[allow(clippy::too_many_arguments)]
fn read_column_range_with_offsets_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Option<String>>, Box<dyn std::error::Error>> {
    if start >= offsets.len() || start >= end {
        return Ok(Vec::new());
    }
    if expected_columns.is_some_and(|expected| column_idx >= expected) {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "CSV column is out of range",
        )));
    }

    let end = end.min(offsets.len());
    let mut values = vec![None; end - start];
    let mut record = ByteRecord::new();
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;

    for (row_index, &row_offset) in offsets.iter().enumerate().take(end).skip(start) {
        if rdr.position().byte() != row_offset {
            let mut position = Position::new();
            position.set_byte(row_offset);
            rdr.seek(position)?;
        }
        if !rdr.read_byte_record(&mut record)? {
            break;
        }
        values[row_index - start] = project_column_from_record(
            &record,
            row_index,
            column_idx,
            settings,
            expected_columns,
            warnings,
        )?;
    }
    Ok(values)
}

fn read_rows_by_index_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    indices: &[usize],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    if indices.is_empty() {
        return Ok(Vec::new());
    }

    let mut record = ByteRecord::new();
    let mut rows = vec![Vec::new(); indices.len()];

    let mut ordered = indices
        .iter()
        .copied()
        .enumerate()
        .map(|(order_idx, row_idx)| (row_idx, order_idx))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(row_idx, _)| *row_idx);

    for (row_index, order_idx) in ordered {
        if row_index >= offsets.len() {
            continue;
        }

        if rdr.position().byte() != offsets[row_index] {
            let mut position = Position::new();
            position.set_byte(offsets[row_index]);
            rdr.seek(position)?;
        }

        if !rdr.read_byte_record(&mut record)? {
            continue;
        }

        let strip_bom = !settings.has_headers && row_index == 0;
        let (decoded, had_errors) = decode_record(&record, settings, strip_bom);
        if had_errors {
            push_warning(
                warnings,
                ParseWarning {
                    record: Some(row_index as u64),
                    line: None,
                    byte: None,
                    field: None,
                    kind: "utf8".to_string(),
                    message: "Record contains invalid encoding".to_string(),
                    expected_len: None,
                    len: None,
                },
            );
            if settings.malformed == MalformedMode::Strict {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("CSV error: record {} has invalid encoding", row_index),
                )));
            }
            if settings.malformed == MalformedMode::Skip {
                continue;
            }
        }

        let decoded = apply_length_policy(
            decoded,
            expected_columns,
            settings,
            Some(row_index as u64),
            warnings,
        )?
        .unwrap_or_default();

        let decoded = enforce_size_limits(decoded, settings, Some(row_index as u64), warnings)?
            .unwrap_or_default();

        rows[order_idx] = decoded;
    }

    Ok(rows)
}

pub fn read_chunk(
    path: impl AsRef<Path>,
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Vec<String>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, settings.has_headers);
    read_chunk_from_reader(rdr, start, count, settings, expected_columns, warnings)
        .map_err(map_boxed_csv_error)
}

pub fn read_chunk_mmap(
    data: &[u8],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Vec<String>>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    read_chunk_from_reader(rdr, start, count, settings, expected_columns, warnings)
        .map_err(map_boxed_csv_error)
}

pub fn read_chunk_with_offsets(
    path: impl AsRef<Path>,
    offsets: &[u64],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Vec<String>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, false);
    read_chunk_with_offsets_from_reader(
        rdr,
        offsets,
        start,
        count,
        settings,
        expected_columns,
        warnings,
    )
    .map_err(map_boxed_csv_error)
}

pub fn read_chunk_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Vec<String>>> {
    let cursor = Cursor::new(data);
    let rdr = build_reader(cursor, settings, false);
    read_chunk_with_offsets_from_reader(
        rdr,
        offsets,
        start,
        count,
        settings,
        expected_columns,
        warnings,
    )
    .map_err(map_boxed_csv_error)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_column_range_with_offsets(
    path: impl AsRef<Path>,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Option<String>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, false);
    read_column_range_with_offsets_from_reader(
        rdr,
        offsets,
        start,
        end,
        column_idx,
        settings,
        expected_columns,
        warnings,
    )
    .map_err(map_boxed_csv_error)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_column_range_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Option<String>>> {
    let cursor = Cursor::new(data);
    let rdr = build_reader(cursor, settings, false);
    read_column_range_with_offsets_from_reader(
        rdr,
        offsets,
        start,
        end,
        column_idx,
        settings,
        expected_columns,
        warnings,
    )
    .map_err(map_boxed_csv_error)
}

pub fn read_rows_by_index(
    path: impl AsRef<Path>,
    offsets: &[u64],
    indices: &[usize],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Vec<String>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, false);
    read_rows_by_index_from_reader(rdr, offsets, indices, settings, expected_columns, warnings)
        .map_err(map_boxed_csv_error)
}

pub fn read_rows_by_index_mmap(
    data: &[u8],
    offsets: &[u64],
    indices: &[usize],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> QuickRowsResult<Vec<Vec<String>>> {
    let cursor = Cursor::new(data);
    let rdr = build_reader(cursor, settings, false);
    read_rows_by_index_from_reader(rdr, offsets, indices, settings, expected_columns, warnings)
        .map_err(map_boxed_csv_error)
}
