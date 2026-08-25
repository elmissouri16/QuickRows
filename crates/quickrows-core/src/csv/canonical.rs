fn needs_prepared_source(settings: &ParseSettings) -> bool {
    settings.source_bom_len > 0
        || settings.encoding == encoding_rs::UTF_16LE
        || settings.encoding == encoding_rs::UTF_16BE
        || !settings.delimiter.is_ascii()
        || !settings.quote.is_ascii()
        || settings.escape.is_some_and(|value| !value.is_ascii())
        || settings.comment.is_some()
        || settings.excel_sep
        || settings.max_field_size != usize::MAX
        || settings.max_record_size != usize::MAX
        || settings.malformed != MalformedMode::Skip
}

trait CanonicalRecordSink {
    type Output;

    fn write_record(&mut self, fields: &[String]) -> QuickRowsResult<()>;
    fn finish(self) -> QuickRowsResult<Self::Output>;
}

struct CsvCanonicalSink<W: Write> {
    writer: csv::Writer<W>,
}

impl<W: Write> CsvCanonicalSink<W> {
    fn new(output: W) -> Self {
        Self {
            writer: csv::WriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .terminator(Terminator::Any(b'\n'))
                .from_writer(output),
        }
    }
}

impl<W: Write> CanonicalRecordSink for CsvCanonicalSink<W> {
    type Output = ();

    fn write_record(&mut self, fields: &[String]) -> QuickRowsResult<()> {
        self.writer
            .write_record(fields)
            .map_err(map_csv_error)
    }

    fn finish(mut self) -> QuickRowsResult<Self::Output> {
        self.writer.flush().map_err(QuickRowsError::from)
    }
}

#[derive(Clone, Default)]
struct ReusableRecordBuffer(Rc<RefCell<Vec<u8>>>);

impl Write for ReusableRecordBuffer {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct SavedCanonicalOutput {
    headers: Vec<String>,
    offsets: Vec<u64>,
}

struct IndexedSavedCsvSink<W: Write> {
    output: W,
    record_writer: csv::Writer<ReusableRecordBuffer>,
    record_bytes: Rc<RefCell<Vec<u8>>>,
    has_headers: bool,
    expected_headers: Vec<String>,
    expected_rows: usize,
    headers: Vec<String>,
    offsets: Vec<u64>,
    records_seen: usize,
    position: u64,
}

impl<W: Write> IndexedSavedCsvSink<W> {
    fn new(
        output: W,
        has_headers: bool,
        expected_headers: &[String],
        expected_rows: usize,
    ) -> Self {
        let record_buffer = ReusableRecordBuffer::default();
        let record_bytes = record_buffer.0.clone();
        Self {
            output,
            record_writer: csv::WriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .terminator(Terminator::Any(b'\n'))
                .from_writer(record_buffer),
            record_bytes,
            has_headers,
            expected_headers: expected_headers.to_vec(),
            expected_rows,
            headers: Vec::new(),
            offsets: Vec::with_capacity(expected_rows),
            records_seen: 0,
            position: 0,
        }
    }

    fn write_canonical_record(&mut self, fields: &[String]) -> QuickRowsResult<()> {
        self.record_writer
            .write_record(fields)
            .map_err(map_csv_error)?;
        self.record_writer
            .flush()
            .map_err(QuickRowsError::from)?;
        let mut bytes = self.record_bytes.borrow_mut();
        self.output
            .write_all(&bytes)
            .map_err(QuickRowsError::from)?;
        self.position = self.position.saturating_add(bytes.len() as u64);
        bytes.clear();
        Ok(())
    }
}

impl<W: Write> CanonicalRecordSink for IndexedSavedCsvSink<W> {
    type Output = SavedCanonicalOutput;

    fn write_record(&mut self, fields: &[String]) -> QuickRowsResult<()> {
        let is_header = self.has_headers && self.records_seen == 0;
        if is_header {
            if fields != self.expected_headers {
                return Err(QuickRowsError::invalid_csv(
                    "Saved CSV headers changed during validation",
                ));
            }
            self.headers = fields.to_vec();
        } else {
            if !self.has_headers && self.headers.is_empty() {
                self.headers = (0..fields.len())
                    .map(|column| format!("Column {}", column + 1))
                    .collect();
                if !self.expected_headers.is_empty() && self.headers != self.expected_headers {
                    return Err(QuickRowsError::invalid_csv(
                        "Saved CSV column count changed during validation",
                    ));
                }
            }
            if fields.len() != self.headers.len() {
                return Err(QuickRowsError::invalid_csv(format!(
                    "Saved CSV row has {} fields, expected {}",
                    fields.len(),
                    self.headers.len()
                )));
            }
            self.offsets.push(self.position);
        }
        self.write_canonical_record(fields)?;
        self.records_seen = self.records_seen.saturating_add(1);
        Ok(())
    }

    fn finish(mut self) -> QuickRowsResult<Self::Output> {
        if self.has_headers && !self.expected_headers.is_empty() && self.records_seen == 0 {
            return Err(QuickRowsError::invalid_csv(
                "Saved CSV is missing its header record",
            ));
        }
        if self.offsets.len() != self.expected_rows {
            return Err(QuickRowsError::invalid_csv(format!(
                "Saved CSV has {} rows, expected {}",
                self.offsets.len(),
                self.expected_rows
            )));
        }
        self.output.flush().map_err(QuickRowsError::from)?;
        Ok(SavedCanonicalOutput {
            headers: self.headers,
            offsets: self.offsets,
        })
    }
}

struct StreamingCanonicalWriter<'a, S: CanonicalRecordSink> {
    settings: &'a ParseSettings,
    sink: S,
    warnings: &'a mut Vec<ParseWarning>,
    comments: &'a mut Vec<PreservedComment>,
    fields: Vec<String>,
    field: String,
    field_size: usize,
    record_size: usize,
    in_quotes: bool,
    pending_quote: bool,
    pending_escape: bool,
    after_quote: bool,
    at_field_start: bool,
    at_record_start: bool,
    skipping_comment: bool,
    comment_text: String,
    emitted_records: usize,
    pending_cr: bool,
    skip_record: bool,
    warned_record: bool,
    record: u64,
    saw_record_content: bool,
    checking_excel_sep: bool,
    excel_prefix: String,
}

impl<'a, S: CanonicalRecordSink> StreamingCanonicalWriter<'a, S> {
    fn new(
        sink: S,
        settings: &'a ParseSettings,
        warnings: &'a mut Vec<ParseWarning>,
        comments: &'a mut Vec<PreservedComment>,
    ) -> Self {
        Self {
            settings,
            sink,
            warnings,
            comments,
            fields: Vec::new(),
            field: String::new(),
            field_size: 0,
            record_size: 0,
            in_quotes: false,
            pending_quote: false,
            pending_escape: false,
            after_quote: false,
            at_field_start: true,
            at_record_start: true,
            skipping_comment: false,
            comment_text: String::new(),
            emitted_records: 0,
            pending_cr: false,
            skip_record: false,
            warned_record: false,
            record: 1,
            saw_record_content: false,
            checking_excel_sep: settings.excel_sep,
            excel_prefix: String::new(),
        }
    }

    fn malformed(&mut self, message: &str, kind: &str) -> QuickRowsResult<()> {
        if self.settings.malformed == MalformedMode::Strict {
            return Err(QuickRowsError::invalid_csv(message));
        }
        if !self.warned_record {
            push_warning(
                self.warnings,
                ParseWarning {
                    record: Some(self.record),
                    line: None,
                    byte: None,
                    field: None,
                    kind: kind.to_string(),
                    message: message.to_string(),
                    expected_len: None,
                    len: None,
                },
            );
            self.warned_record = true;
        }
        if self.settings.malformed == MalformedMode::Skip {
            self.skip_record = true;
        }
        Ok(())
    }

    fn limit_exceeded(
        &mut self,
        kind: &str,
        message: String,
        limit: usize,
        len: usize,
    ) -> QuickRowsResult<()> {
        if self.settings.malformed == MalformedMode::Strict {
            return Err(QuickRowsError::invalid_csv(message));
        }
        if !self.warned_record {
            push_warning(
                self.warnings,
                ParseWarning {
                    record: Some(self.record),
                    line: None,
                    byte: None,
                    field: None,
                    kind: if self.settings.malformed == MalformedMode::Repair {
                        "repaired".to_string()
                    } else {
                        kind.to_string()
                    },
                    message,
                    expected_len: Some(limit as u64),
                    len: Some(len as u64),
                },
            );
            self.warned_record = true;
        }
        if self.settings.malformed == MalformedMode::Skip {
            self.skip_record = true;
        }
        Ok(())
    }

    fn push_field_char(&mut self, ch: char) -> QuickRowsResult<()> {
        let bytes = ch.len_utf8();
        self.field_size = self.field_size.saturating_add(bytes);
        let field_too_large = self.field_size > self.settings.max_field_size;
        if field_too_large {
            self.limit_exceeded(
                "max-field-size",
                format!(
                    "Field exceeds max size ({} bytes)",
                    self.settings.max_field_size
                ),
                self.settings.max_field_size,
                self.field_size,
            )?;
            return Ok(());
        }

        let next_record_size = self.record_size.saturating_add(bytes);
        let record_too_large = next_record_size > self.settings.max_record_size;
        if record_too_large {
            self.limit_exceeded(
                "max-record-size",
                format!(
                    "Record exceeds max size ({} bytes)",
                    self.settings.max_record_size
                ),
                self.settings.max_record_size,
                next_record_size,
            )?;
            return Ok(());
        }

        self.record_size = next_record_size;
        if !self.skip_record {
            self.field.push(ch);
        }
        Ok(())
    }

    fn finish_record(&mut self) -> QuickRowsResult<()> {
        self.fields.push(std::mem::take(&mut self.field));
        if !self.skip_record {
            self.sink.write_record(&self.fields)?;
            self.emitted_records += 1;
        }
        self.fields.clear();
        self.field_size = 0;
        self.record_size = 0;
        self.in_quotes = false;
        self.pending_quote = false;
        self.pending_escape = false;
        self.after_quote = false;
        self.at_field_start = true;
        self.at_record_start = true;
        self.skip_record = false;
        self.warned_record = false;
        self.saw_record_content = false;
        self.record += 1;
        Ok(())
    }

    fn finish_comment(&mut self) {
        if !self.comment_text.is_empty() {
            self.comments.push(PreservedComment {
                before_record: self.emitted_records,
                text: std::mem::take(&mut self.comment_text),
            });
        }
        self.reset_comment();
    }

    fn reset_comment(&mut self) {
        self.field.clear();
        self.fields.clear();
        self.field_size = 0;
        self.record_size = 0;
        self.in_quotes = false;
        self.pending_quote = false;
        self.pending_escape = false;
        self.after_quote = false;
        self.at_field_start = true;
        self.at_record_start = true;
        self.skipping_comment = false;
        self.skip_record = false;
        self.warned_record = false;
        self.saw_record_content = false;
    }

    fn process_char(&mut self, ch: char) -> QuickRowsResult<()> {
        if self.pending_cr {
            self.pending_cr = false;
            if ch == '\n' {
                return Ok(());
            }
        }

        if self.skipping_comment {
            if ch == '\r' || ch == '\n' {
                self.finish_comment();
                self.pending_cr = ch == '\r';
            } else {
                self.comment_text.push(ch);
            }
            return Ok(());
        }

        loop {
            if self.in_quotes {
                if self.pending_escape {
                    self.pending_escape = false;
                    if ch == self.settings.quote {
                        self.push_field_char(ch)?;
                        self.saw_record_content = true;
                        return Ok(());
                    }
                    if let Some(escape) = self.settings.escape {
                        self.push_field_char(escape)?;
                    }
                }
                if self.pending_quote {
                    self.pending_quote = false;
                    if ch == self.settings.quote {
                        self.push_field_char(ch)?;
                        self.saw_record_content = true;
                        return Ok(());
                    }
                    self.in_quotes = false;
                    self.after_quote = true;
                    continue;
                }
                if self.settings.escape == Some(ch) {
                    self.pending_escape = true;
                } else if ch == self.settings.quote {
                    self.pending_quote = true;
                } else {
                    self.push_field_char(ch)?;
                    self.saw_record_content = true;
                }
                return Ok(());
            }

            if self.after_quote {
                if ch != self.settings.delimiter && ch != '\r' && ch != '\n' {
                    self.malformed(
                        "CSV quote must be followed by a delimiter or record ending",
                        "malformed-quote",
                    )?;
                    self.push_field_char(ch)?;
                    self.after_quote = false;
                    self.at_field_start = false;
                    self.at_record_start = false;
                    self.saw_record_content = true;
                    return Ok(());
                }
                self.after_quote = false;
            }

            if self.at_record_start && self.settings.comment == Some(ch) {
                self.skipping_comment = true;
                self.comment_text.push(ch);
            } else if ch == self.settings.quote && self.at_field_start {
                self.in_quotes = true;
                self.at_field_start = false;
                self.saw_record_content = true;
            } else if ch == self.settings.delimiter {
                self.fields.push(std::mem::take(&mut self.field));
                self.field_size = 0;
                self.at_field_start = true;
                self.at_record_start = false;
                self.saw_record_content = true;
            } else if ch == '\r' || ch == '\n' {
                self.finish_record()?;
                self.pending_cr = ch == '\r';
            } else {
                if ch == self.settings.quote {
                    self.malformed(
                        "CSV quote is only valid at the start of a field",
                        "malformed-quote",
                    )?;
                }
                self.push_field_char(ch)?;
                self.at_field_start = false;
                self.at_record_start = false;
                self.saw_record_content = true;
            }
            return Ok(());
        }
    }

    fn feed(&mut self, text: &str) -> QuickRowsResult<()> {
        for ch in text.chars() {
            if self.checking_excel_sep {
                self.excel_prefix.push(ch);
                let line_finished = ch == '\r' || ch == '\n';
                let too_long = self.excel_prefix.chars().count() > 16;
                if !line_finished && !too_long {
                    continue;
                }
                self.checking_excel_sep = false;
                let prefix = std::mem::take(&mut self.excel_prefix);
                let line = prefix.trim_end_matches(['\r', '\n']);
                let is_directive = line
                    .strip_prefix("sep=")
                    .and_then(one_scalar)
                    .is_some_and(|delimiter| delimiter == self.settings.delimiter);
                if is_directive {
                    self.pending_cr = ch == '\r';
                    continue;
                }
                for buffered in prefix.chars() {
                    self.process_char(buffered)?;
                }
                continue;
            }
            self.process_char(ch)?;
        }
        Ok(())
    }

    fn finish(mut self) -> QuickRowsResult<S::Output> {
        if self.checking_excel_sep {
            self.checking_excel_sep = false;
            let prefix = std::mem::take(&mut self.excel_prefix);
            let is_directive = prefix
                .strip_prefix("sep=")
                .and_then(one_scalar)
                .is_some_and(|delimiter| delimiter == self.settings.delimiter);
            if !is_directive {
                for ch in prefix.chars() {
                    self.process_char(ch)?;
                }
            }
        }
        if self.skipping_comment {
            self.finish_comment();
        } else {
            if self.pending_escape {
                if let Some(escape) = self.settings.escape {
                    self.push_field_char(escape)?;
                }
                self.pending_escape = false;
            }
            if self.pending_quote {
                self.pending_quote = false;
                self.in_quotes = false;
                self.after_quote = true;
            }
            if self.in_quotes {
                self.malformed("CSV quoted field is not closed", "malformed-quote")?;
                self.in_quotes = false;
            }
            if self.saw_record_content || !self.field.is_empty() || !self.fields.is_empty() {
                self.finish_record()?;
            }
        }
        self.sink.finish()
    }
}

#[derive(Default)]
struct RawStreamFingerprint {
    hasher: blake3::Hasher,
    len: u64,
}

impl RawStreamFingerprint {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        self.len = self.len.saturating_add(bytes.len() as u64);
    }

    fn finish(self) -> (u64, [u8; 32]) {
        (self.len, *self.hasher.finalize().as_bytes())
    }
}

#[allow(clippy::too_many_arguments)]
fn stream_canonical_csv_to_sink<S: CanonicalRecordSink>(
    path: &Path,
    settings: &ParseSettings,
    sink: S,
    warnings: &mut Vec<ParseWarning>,
    comments: &mut Vec<PreservedComment>,
    progress: Option<&dyn Fn(usize)>,
    is_cancelled: &dyn Fn() -> bool,
    mut raw_fingerprint: Option<&mut RawStreamFingerprint>,
) -> QuickRowsResult<S::Output> {
    let mut file = File::open(path).map_err(QuickRowsError::from)?;
    if settings.source_bom_len > 0 {
        let mut bom = vec![0u8; settings.source_bom_len];
        file.read_exact(&mut bom)
            .map_err(QuickRowsError::from)?;
        if let Some(fingerprint) = raw_fingerprint.as_deref_mut() {
            fingerprint.update(&bom);
        }
    }
    let mut reader = BufReader::new(file);
    let mut decoder = settings.encoding.new_decoder_without_bom_handling();
    let mut canonical = StreamingCanonicalWriter::new(sink, settings, warnings, comments);
    let mut input = vec![0u8; 64 * 1024];
    let mut decoded = vec![0u8; 256 * 1024 + 16];
    let mut total_read = settings.source_bom_len;

    loop {
        if is_cancelled() {
            return Err(QuickRowsError::cancelled());
        }
        let read = reader.read(&mut input).map_err(QuickRowsError::from)?;
        if let Some(fingerprint) = raw_fingerprint.as_deref_mut() {
            fingerprint.update(&input[..read]);
        }
        total_read += read;
        let last = read == 0;
        let mut consumed = 0;
        loop {
            if is_cancelled() {
                return Err(QuickRowsError::cancelled());
            }
            let (result, used, written) = decoder.decode_to_utf8_without_replacement(
                &input[consumed..read],
                &mut decoded,
                last,
            );
            consumed += used;
            let text = std::str::from_utf8(&decoded[..written])
                .map_err(|_| QuickRowsError::invalid_csv("Decoder produced invalid UTF-8"))?;
            canonical.feed(text)?;
            match result {
                encoding_rs::DecoderResult::InputEmpty => break,
                encoding_rs::DecoderResult::OutputFull => continue,
                encoding_rs::DecoderResult::Malformed(_, _) => {
                    canonical.malformed(
                        &format!(
                            "Invalid byte sequence in {} CSV input",
                            settings.encoding_label
                        ),
                        "encoding",
                    )?;
                    canonical.feed("\u{fffd}")?;
                }
            }
        }
        if let Some(progress) = progress {
            progress(total_read);
        }
        if last {
            break;
        }
    }
    canonical.finish()
}

fn stream_canonical_csv<W: Write>(
    path: &Path,
    settings: &ParseSettings,
    output: W,
    warnings: &mut Vec<ParseWarning>,
    comments: &mut Vec<PreservedComment>,
    progress: Option<&dyn Fn(usize)>,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<()> {
    stream_canonical_csv_to_sink(
        path,
        settings,
        CsvCanonicalSink::new(output),
        warnings,
        comments,
        progress,
        is_cancelled,
        None,
    )
}

fn csv_tempfile_near(path: &Path, prefix: &str) -> QuickRowsResult<tempfile::NamedTempFile> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix).suffix(".csv");
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        && let Ok(temporary) = builder.tempfile_in(parent)
    {
        return Ok(temporary);
    }
    builder.tempfile().map_err(QuickRowsError::from)
}

pub fn prepare_csv_source(
    path: &Path,
    settings: &ParseSettings,
) -> QuickRowsResult<PreparedCsvSource> {
    prepare_csv_source_cancellable(path, settings, None, &|| false)
}

pub fn prepare_csv_source_cancellable(
    path: &Path,
    settings: &ParseSettings,
    progress: Option<&dyn Fn(usize)>,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<PreparedCsvSource> {
    if is_cancelled() {
        return Err(QuickRowsError::cancelled());
    }
    if !needs_prepared_source(settings) {
        return Ok(PreparedCsvSource {
            path: path.to_path_buf(),
            settings: settings.clone(),
            temporary: None,
            warnings: Vec::new(),
            comments: Vec::new(),
        });
    }

    let mut temporary = csv_tempfile_near(path, "quickrows-decoded-")?;
    let mut warnings = Vec::new();
    let mut comments = Vec::new();
    stream_canonical_csv(
        path,
        settings,
        temporary.as_file_mut(),
        &mut warnings,
        &mut comments,
        progress,
        is_cancelled,
    )?;
    temporary
        .as_file_mut()
        .flush()
        .map_err(QuickRowsError::from)?;

    Ok(PreparedCsvSource {
        path: temporary.path().to_path_buf(),
        settings: canonical_storage_settings(settings),
        temporary: Some(Arc::new(temporary)),
        warnings,
        comments,
    })
}

pub(crate) fn prepare_saved_csv_source_cancellable(
    raw_path: &Path,
    strict_settings: &ParseSettings,
    expected_headers: &[String],
    expected_rows: usize,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<PreparedSavedCsvSource> {
    if strict_settings.malformed != MalformedMode::Strict {
        return Err(QuickRowsError::invalid_csv(
            "Saved CSV validation requires strict malformed-row handling",
        ));
    }
    if is_cancelled() {
        return Err(QuickRowsError::cancelled());
    }

    let mut temporary = csv_tempfile_near(raw_path, "quickrows-saved-")?;
    let mut warnings = Vec::new();
    let mut comments = Vec::new();
    let output = BufWriter::with_capacity(1024 * 1024, temporary.as_file_mut());
    let indexed = IndexedSavedCsvSink::new(
        output,
        strict_settings.has_headers,
        expected_headers,
        expected_rows,
    );
    let mut raw_fingerprint = RawStreamFingerprint::default();
    let result = stream_canonical_csv_to_sink(
        raw_path,
        strict_settings,
        indexed,
        &mut warnings,
        &mut comments,
        None,
        is_cancelled,
        Some(&mut raw_fingerprint),
    )?;
    if !warnings.is_empty() {
        return Err(QuickRowsError::invalid_csv(
            "Saved CSV produced warnings during strict validation",
        ));
    }
    if is_cancelled() {
        return Err(QuickRowsError::cancelled());
    }

    let path = temporary.path().to_path_buf();
    let (raw_len, raw_content_hash) = raw_fingerprint.finish();
    Ok(PreparedSavedCsvSource {
        prepared: PreparedCsvSource {
            path,
            settings: canonical_storage_settings(strict_settings),
            temporary: Some(Arc::new(temporary)),
            warnings,
            comments,
        },
        headers: result.headers,
        offsets: result.offsets,
        raw_len,
        raw_content_hash,
    })
}
