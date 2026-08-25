fn map_offset_error(error: Box<dyn std::error::Error>) -> QuickRowsError {
    if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
        error.kind() == std::io::ErrorKind::Interrupted
            && error.to_string() == "Operation cancelled"
    }) {
        QuickRowsError::cancelled()
    } else {
        map_boxed_csv_error(error)
    }
}

fn build_row_offsets_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
    cancel_cb: Option<&dyn Fn() -> bool>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    if settings.has_headers {
        let _ = rdr.byte_headers().inspect_err(|err| {
            push_warning(warnings, warning_from_error(err, None));
        })?;
    }

    let mut offsets = Vec::new();
    let mut record = ByteRecord::new();
    let mut row_index: u64 = 0;
    loop {
        if cancel_cb.is_some_and(|cancelled| cancelled()) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "Operation cancelled",
            )));
        }
        let pos = rdr.position().byte();
        match rdr.read_byte_record(&mut record) {
            Ok(false) => break,
            Ok(true) => {
                let mut skip_row = false;
                let has_decode_errors =
                    record.iter().any(|field| settings.encoding.decode(field).2);
                if has_decode_errors {
                    push_warning(
                        warnings,
                        ParseWarning {
                            record: Some(row_index),
                            line: None,
                            byte: Some(pos),
                            field: None,
                            kind: "encoding".to_string(),
                            message: format!(
                                "Record contains bytes that are invalid in {}",
                                settings.encoding_label
                            ),
                            expected_len: None,
                            len: None,
                        },
                    );
                    match settings.malformed {
                        MalformedMode::Strict => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!(
                                    "CSV error: record {} contains invalid {} data",
                                    row_index, settings.encoding_label
                                ),
                            )));
                        }
                        MalformedMode::Skip => skip_row = true,
                        MalformedMode::Repair => {}
                    }
                }
                if let Some(expected) = expected_columns
                    && record.len() != expected
                {
                    match settings.malformed {
                        MalformedMode::Strict => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!(
                                    "CSV error: record {} has {} fields, expected {}",
                                    row_index,
                                    record.len(),
                                    expected
                                ),
                            )));
                        }
                        MalformedMode::Skip => {
                            skip_row = true;
                            push_warning(
                                warnings,
                                ParseWarning {
                                    record: Some(row_index),
                                    line: None,
                                    byte: Some(pos),
                                    field: None,
                                    kind: "unequal-lengths".to_string(),
                                    message: format!(
                                        "Record has {} fields, expected {}",
                                        record.len(),
                                        expected
                                    ),
                                    expected_len: Some(expected as u64),
                                    len: Some(record.len() as u64),
                                },
                            );
                        }
                        MalformedMode::Repair => {
                            push_warning(
                                warnings,
                                ParseWarning {
                                    record: Some(row_index),
                                    line: None,
                                    byte: Some(pos),
                                    field: None,
                                    kind: "repaired".to_string(),
                                    message: format!(
                                        "Record length adjusted to {} fields",
                                        expected
                                    ),
                                    expected_len: Some(expected as u64),
                                    len: Some(record.len() as u64),
                                },
                            );
                        }
                    }
                }

                let mut total = 0usize;
                for field in record.iter() {
                    total += field.len();
                    if field.len() > settings.max_field_size {
                        match settings.malformed {
                            MalformedMode::Strict => {
                                return Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!(
                                        "CSV error: record {} field exceeds max size ({})",
                                        row_index, settings.max_field_size
                                    ),
                                )));
                            }
                            MalformedMode::Skip => {
                                skip_row = true;
                                push_warning(
                                    warnings,
                                    ParseWarning {
                                        record: Some(row_index),
                                        line: None,
                                        byte: Some(pos),
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
                                break;
                            }
                            MalformedMode::Repair => {
                                push_warning(
                                    warnings,
                                    ParseWarning {
                                        record: Some(row_index),
                                        line: None,
                                        byte: Some(pos),
                                        field: None,
                                        kind: "repaired".to_string(),
                                        message: "Field truncated to fit size limit".to_string(),
                                        expected_len: None,
                                        len: Some(field.len() as u64),
                                    },
                                );
                            }
                        }
                    }
                }

                if total > settings.max_record_size {
                    match settings.malformed {
                        MalformedMode::Strict => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!(
                                    "CSV error: record {} exceeds max size ({})",
                                    row_index, settings.max_record_size
                                ),
                            )));
                        }
                        MalformedMode::Skip => {
                            skip_row = true;
                            push_warning(
                                warnings,
                                ParseWarning {
                                    record: Some(row_index),
                                    line: None,
                                    byte: Some(pos),
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
                        }
                        MalformedMode::Repair => {
                            push_warning(
                                warnings,
                                ParseWarning {
                                    record: Some(row_index),
                                    line: None,
                                    byte: Some(pos),
                                    field: None,
                                    kind: "repaired".to_string(),
                                    message: "Record truncated to fit size limit".to_string(),
                                    expected_len: None,
                                    len: Some(total as u64),
                                },
                            );
                        }
                    }
                }

                if !skip_row {
                    offsets.push(pos);
                }
                if row_index.is_multiple_of(10000)
                    && let Some(cb) = progress_cb
                {
                    cb(row_index as usize);
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
                continue;
            }
        }
    }
    Ok(offsets)
}

pub fn build_row_offsets(
    path: impl AsRef<Path>,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> QuickRowsResult<Vec<u64>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, settings.has_headers);
    build_row_offsets_from_reader(rdr, settings, expected_columns, warnings, progress_cb, None)
        .map_err(map_offset_error)
}

pub fn build_row_offsets_cancellable(
    path: impl AsRef<Path>,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
    cancel_cb: &dyn Fn() -> bool,
) -> QuickRowsResult<Vec<u64>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, settings.has_headers);
    build_row_offsets_from_reader(
        rdr,
        settings,
        expected_columns,
        warnings,
        progress_cb,
        Some(cancel_cb),
    )
    .map_err(map_offset_error)
}

pub fn build_row_offsets_mmap(
    data: &[u8],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> QuickRowsResult<Vec<u64>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    build_row_offsets_from_reader(rdr, settings, expected_columns, warnings, progress_cb, None)
        .map_err(map_offset_error)
}

pub fn build_row_offsets_mmap_cancellable(
    data: &[u8],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
    cancel_cb: &dyn Fn() -> bool,
) -> QuickRowsResult<Vec<u64>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    build_row_offsets_from_reader(
        rdr,
        settings,
        expected_columns,
        warnings,
        progress_cb,
        Some(cancel_cb),
    )
    .map_err(map_offset_error)
}
