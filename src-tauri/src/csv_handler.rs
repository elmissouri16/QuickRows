use chardetng::EncodingDetector;
use csv::{ByteRecord, Position, ReaderBuilder, StringRecord, Terminator};
use encoding_rs::Encoding;
use rayon::prelude::*;
use rayon::slice::ParallelSliceMut;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};

const SAMPLE_SIZE: usize = 64 * 1024;
pub const MAX_WARNING_COUNT: usize = 200;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MalformedMode {
    Strict,
    Skip,
    Repair,
}

impl MalformedMode {
    fn from_str(value: &str) -> Self {
        match value {
            "skip" => MalformedMode::Skip,
            "repair" => MalformedMode::Repair,
            _ => MalformedMode::Strict,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            MalformedMode::Strict => "strict",
            MalformedMode::Skip => "skip",
            MalformedMode::Repair => "repair",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ParseOverrides {
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub line_ending: Option<String>,
    pub encoding: Option<String>,
    pub has_headers: Option<bool>,
    pub malformed: Option<String>,
    pub max_field_size: Option<usize>,
    pub max_record_size: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ParseInfo {
    pub delimiter: String,
    pub quote: String,
    pub escape: Option<String>,
    pub line_ending: String,
    pub encoding: String,
    pub has_headers: bool,
    pub malformed: String,
    pub max_field_size: usize,
    pub max_record_size: usize,
}

#[derive(Clone, Debug)]
pub struct ParseSettings {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub terminator: Terminator,
    pub line_ending: String,
    pub has_headers: bool,
    pub encoding: &'static Encoding,
    pub encoding_label: String,
    pub malformed: MalformedMode,
    pub max_field_size: usize,
    pub max_record_size: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ParseWarning {
    pub record: Option<u64>,
    pub line: Option<u64>,
    pub byte: Option<u64>,
    pub field: Option<u64>,
    pub kind: String,
    pub message: String,
    pub expected_len: Option<u64>,
    pub len: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct DetectedSettings {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub line_ending: String,
    pub encoding: &'static Encoding,
    pub encoding_label: String,
    pub has_headers: bool,
}

fn push_warning(warnings: &mut Vec<ParseWarning>, warning: ParseWarning) {
    if warnings.len() >= MAX_WARNING_COUNT {
        return;
    }
    warnings.push(warning);
}

fn warning_from_error(err: &csv::Error, record: Option<u64>) -> ParseWarning {
    let mut warning = ParseWarning {
        record,
        line: None,
        byte: None,
        field: None,
        kind: "parse".to_string(),
        message: err.to_string(),
        expected_len: None,
        len: None,
    };

    if let Some(pos) = err.position() {
        warning.record = warning.record.or(Some(pos.record()));
        warning.line = Some(pos.line());
        warning.byte = Some(pos.byte());
    }

    match err.kind() {
        csv::ErrorKind::UnequalLengths { expected_len, len, .. } => {
            warning.kind = "unequal-lengths".to_string();
            warning.expected_len = Some(*expected_len);
            warning.len = Some(*len);
        }
        csv::ErrorKind::Utf8 { err, .. } => {
            warning.kind = "utf8".to_string();
            warning.field = Some(err.field() as u64);
        }
        _ => {}
    }

    warning
}

fn normalize_delimiter(value: &str) -> Option<u8> {
    match value.trim().to_lowercase().as_str() {
        "comma" | "," => Some(b','),
        "tab" | "\\t" | "tsv" => Some(b'\t'),
        "semicolon" | ";" => Some(b';'),
        "pipe" | "|" => Some(b'|'),
        "space" | " " => Some(b' '),
        value if value.len() == 1 => value.as_bytes().first().copied(),
        _ => None,
    }
}

fn normalize_quote(value: &str) -> Option<u8> {
    match value.trim().to_lowercase().as_str() {
        "double" | "\"" => Some(b'"'),
        "single" | "'" => Some(b'\''),
        value if value.len() == 1 => value.as_bytes().first().copied(),
        _ => None,
    }
}

fn normalize_escape(value: &str) -> Option<Option<u8>> {
    match value.trim().to_lowercase().as_str() {
        "none" | "off" => Some(None),
        "backslash" | "\\\\" | "\\" => Some(Some(b'\\')),
        value if value.len() == 1 => value.as_bytes().first().map(|byte| Some(*byte)),
        _ => None,
    }
}

fn normalize_line_ending(value: &str) -> Option<(Terminator, String)> {
    let normalized = value.trim().to_lowercase();
    let result = match normalized.as_str() {
        "lf" | "\\n" => (Terminator::Any(b'\n'), "lf".to_string()),
        "cr" | "\\r" => (Terminator::Any(b'\r'), "cr".to_string()),
        "crlf" | "\\r\\n" => (Terminator::CRLF, "crlf".to_string()),
        "auto" => (Terminator::CRLF, "auto".to_string()),
        _ => return None,
    };
    Some(result)
}

fn detect_line_ending(sample: &[u8]) -> String {
    let mut crlf = 0;
    let mut lf = 0;
    let mut cr = 0;
    let mut idx = 0;
    while idx < sample.len() {
        match sample[idx] {
            b'\r' => {
                if idx + 1 < sample.len() && sample[idx + 1] == b'\n' {
                    crlf += 1;
                    idx += 2;
                    continue;
                }
                cr += 1;
            }
            b'\n' => {
                lf += 1;
            }
            _ => {}
        }
        idx += 1;
    }
    if crlf >= lf && crlf >= cr {
        "crlf".to_string()
    } else if lf >= cr {
        "lf".to_string()
    } else {
        "cr".to_string()
    }
}

fn detect_quote_char(sample: &str) -> u8 {
    let double_count = sample.matches('"').count();
    let single_count = sample.matches('\'').count();
    if double_count > 0 && double_count >= single_count {
        b'"'
    } else if single_count > 0 {
        b'\''
    } else {
        b'"'
    }
}

fn count_fields(line: &str, delimiter: char, quote: char) -> Option<usize> {
    if line.trim().is_empty() {
        return None;
    }
    let mut count = 1;
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == quote {
            if in_quotes && chars.peek() == Some(&quote) {
                chars.next();
            } else {
                in_quotes = !in_quotes;
            }
            continue;
        }
        if ch == delimiter && !in_quotes {
            count += 1;
        }
    }
    if in_quotes {
        None
    } else {
        Some(count)
    }
}

fn detect_delimiter(sample: &str, quote: u8) -> u8 {
    let quote_char = quote as char;
    let candidates = [',', '\t', ';', '|'];
    let lines = sample
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(20)
        .collect::<Vec<_>>();
    let mut best = (b',', 0usize, 0usize);

    for candidate in candidates {
        let mut counts: Vec<usize> = Vec::new();
        for line in &lines {
            if let Some(count) = count_fields(line, candidate, quote_char) {
                counts.push(count);
            }
        }
        if counts.len() < 2 {
            continue;
        }
        let mut freq = std::collections::HashMap::<usize, usize>::new();
        for count in counts {
            *freq.entry(count).or_insert(0) += 1;
        }
        if let Some((mode_count, mode_freq)) = freq
            .into_iter()
            .max_by_key(|(count, freq)| (*freq, *count))
        {
            if mode_count > 1 && mode_freq > best.2 {
                best = (candidate as u8, mode_count, mode_freq);
            }
        }
    }

    best.0
}

fn detect_escape(sample: &str, quote: u8) -> Option<u8> {
    let quote_char = quote as char;
    let needle = format!("\\{}", quote_char);
    if sample.contains(&needle) {
        Some(b'\\')
    } else {
        None
    }
}

fn read_sample(path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut sample = vec![0; SAMPLE_SIZE];
    let read = file.read(&mut sample)?;
    sample.truncate(read);
    Ok(sample)
}

fn detect_encoding(sample: &[u8]) -> (&'static Encoding, String, usize) {
    if let Some((encoding, bom_len)) = Encoding::for_bom(sample) {
        return (encoding, encoding.name().to_string(), bom_len);
    }

    let mut detector = EncodingDetector::new();
    detector.feed(sample, true);
    let encoding = detector.guess(None, true);
    (encoding, encoding.name().to_string(), 0)
}

fn is_numeric(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.parse::<f64>().is_ok()
}

fn looks_like_header(first: &StringRecord, second: &StringRecord) -> bool {
    let first_numeric = first.iter().filter(|value| is_numeric(value)).count();
    let second_numeric = second.iter().filter(|value| is_numeric(value)).count();
    let first_len = usize::max(1, first.len());
    let second_len = usize::max(1, second.len());
    let first_ratio = first_numeric as f32 / first_len as f32;
    let second_ratio = second_numeric as f32 / second_len as f32;

    if first_ratio < 0.2 && second_ratio > 0.4 {
        return true;
    }

    let unique = first
        .iter()
        .collect::<HashSet<_>>()
        .len()
        == first.len();
    let first_alpha = first.iter().all(|value| {
        value
            .chars()
            .all(|ch| ch.is_alphabetic() || ch == '_' || ch == '-' || ch == ' ')
    });
    if unique && first_alpha && second_ratio > first_ratio {
        return true;
    }

    false
}

pub fn detect_parse_settings(path: &str) -> Result<DetectedSettings, Box<dyn std::error::Error>> {
    let sample = read_sample(path)?;
    let (encoding, encoding_label, bom_len) = detect_encoding(&sample);
    let sample_no_bom = sample.get(bom_len..).unwrap_or(&sample);
    let (decoded, _, _) = encoding.decode(sample_no_bom);
    let decoded = decoded.into_owned();

    let line_ending = detect_line_ending(sample_no_bom);
    let quote = detect_quote_char(&decoded);
    let delimiter = detect_delimiter(&decoded, quote);
    let escape = detect_escape(&decoded, quote);

    let mut builder = ReaderBuilder::new();
    builder
        .has_headers(false)
        .delimiter(delimiter)
        .quote(quote)
        .escape(escape)
        .terminator(Terminator::CRLF)
        .flexible(true);
    let mut rdr = builder.from_reader(decoded.as_bytes());
    let mut rows = rdr.records();
    let first = rows.next().and_then(|row| row.ok());
    let second = rows.next().and_then(|row| row.ok());
    let has_headers = match (first, second) {
        (Some(first), Some(second)) => looks_like_header(&first, &second),
        _ => true,
    };

    Ok(DetectedSettings {
        delimiter,
        quote,
        escape,
        line_ending,
        encoding,
        encoding_label,
        has_headers,
    })
}

fn format_char(value: u8) -> String {
    (value as char).to_string()
}

pub fn parse_info_from_settings(settings: &ParseSettings) -> ParseInfo {
    ParseInfo {
        delimiter: format_char(settings.delimiter),
        quote: format_char(settings.quote),
        escape: settings.escape.map(format_char),
        line_ending: settings.line_ending.clone(),
        encoding: settings.encoding_label.clone(),
        has_headers: settings.has_headers,
        malformed: settings.malformed.as_str().to_string(),
        max_field_size: settings.max_field_size,
        max_record_size: settings.max_record_size,
    }
}

pub fn apply_parse_overrides(
    detected: &DetectedSettings,
    overrides: Option<ParseOverrides>,
) -> ParseSettings {
    let mut delimiter = detected.delimiter;
    let mut quote = detected.quote;
    let mut escape = detected.escape;
    let mut terminator = Terminator::CRLF;
    let mut line_ending = detected.line_ending.clone();
    let mut encoding = detected.encoding;
    let mut encoding_label = detected.encoding_label.clone();
    let mut has_headers = detected.has_headers;
    let mut malformed = MalformedMode::Skip;
    let mut max_field_size = 256 * 1024;
    let mut max_record_size = 2 * 1024 * 1024;

    if let Some(overrides) = overrides {
        if let Some(value) = overrides.delimiter.as_deref() {
            if let Some(parsed) = normalize_delimiter(value) {
                delimiter = parsed;
            }
        }
        if let Some(value) = overrides.quote.as_deref() {
            if let Some(parsed) = normalize_quote(value) {
                quote = parsed;
            }
        }
        if let Some(value) = overrides.escape.as_deref() {
            if let Some(parsed) = normalize_escape(value) {
                escape = parsed;
            }
        }
        if let Some(value) = overrides.line_ending.as_deref() {
            if let Some((term, ending)) = normalize_line_ending(value) {
                terminator = term;
                line_ending = ending;
            }
        } else {
            if let Some((term, _)) = normalize_line_ending(&detected.line_ending) {
                terminator = term;
            }
        }
        if let Some(value) = overrides.encoding.as_deref() {
            let normalized = match value.trim().to_lowercase().as_str() {
                "latin1" => "iso-8859-1".to_string(),
                "latin-1" => "iso-8859-1".to_string(),
                "utf8" => "utf-8".to_string(),
                other => other.to_string(),
            };
            if let Some(enc) = Encoding::for_label(normalized.as_bytes()) {
                encoding = enc;
                encoding_label = enc.name().to_string();
            }
        }
        if let Some(value) = overrides.has_headers {
            has_headers = value;
        }
        if let Some(value) = overrides.malformed.as_deref() {
            malformed = MalformedMode::from_str(value);
        }
        if let Some(value) = overrides.max_field_size {
            max_field_size = value;
        }
        if let Some(value) = overrides.max_record_size {
            max_record_size = value;
        }
    } else if let Some((term, _)) = normalize_line_ending(&detected.line_ending) {
        terminator = term;
    }

    ParseSettings {
        delimiter,
        quote,
        escape,
        terminator,
        line_ending,
        has_headers,
        encoding,
        encoding_label,
        malformed,
        max_field_size,
        max_record_size,
    }
}

pub fn default_parse_settings() -> ParseSettings {
    ParseSettings {
        delimiter: b',',
        quote: b'"',
        escape: None,
        terminator: Terminator::CRLF,
        line_ending: "auto".to_string(),
        has_headers: true,
        encoding: encoding_rs::UTF_8,
        encoding_label: "utf-8".to_string(),
        malformed: MalformedMode::Skip,
        max_field_size: 256 * 1024,
        max_record_size: 2 * 1024 * 1024,
    }
}

pub fn settings_cache_hash(settings: &ParseSettings) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    settings.delimiter.hash(&mut hasher);
    settings.quote.hash(&mut hasher);
    settings.escape.hash(&mut hasher);
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
    builder
        .delimiter(settings.delimiter)
        .quote(settings.quote)
        .escape(settings.escape)
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
) -> Result<Option<Vec<String>>, Box<dyn std::error::Error>> {
    let mut total = 0usize;
    let mut truncated = false;
    for field in &mut fields {
        if field.len() > settings.max_field_size {
            match settings.malformed {
                MalformedMode::Strict => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "CSV error: record {:?} field exceeds max size ({} bytes)",
                            row_index, settings.max_field_size
                        ),
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
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "CSV error: record {:?} exceeds max size ({} bytes)",
                        row_index, settings.max_record_size
                    ),
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
                while total > settings.max_record_size && !fields.is_empty() {
                    if let Some(last) = fields.pop() {
                        total = total.saturating_sub(last.len());
                    }
                }
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
) -> Result<Option<Vec<String>>, Box<dyn std::error::Error>> {
    if let Some(expected) = expected_columns {
        if fields.len() != expected {
            match settings.malformed {
                MalformedMode::Strict => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "CSV error: record {:?} has {} fields, expected {}",
                            row_index,
                            fields.len(),
                            expected
                        ),
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
                        repaired
                            .extend(std::iter::repeat(String::new()).take(expected - repaired.len()));
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
                            message: format!(
                                "Record length adjusted to {} fields",
                                expected
                            ),
                            expected_len: Some(expected as u64),
                            len: Some(repaired.len() as u64),
                        },
                    );
                    return Ok(Some(repaired));
                }
            }
        }
    }
    Ok(Some(fields))
}

pub fn get_headers(
    path: &str,
    settings: &ParseSettings,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = build_reader(file, settings, settings.has_headers);

    if settings.has_headers {
        let headers = rdr
            .byte_headers()
            .map_err(|err| {
                push_warning(warnings, warning_from_error(&err, None));
                err
            })?
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
        .map_err(|err| {
            push_warning(warnings, warning_from_error(&err, None));
            err
        })?
    {
        let (decoded, _) = decode_record(&record, settings, true);
        let headers = (0..decoded.len())
            .map(|idx| format!("Column {}", idx + 1))
            .collect::<Vec<_>>();
        return Ok(headers);
    }

    Ok(Vec::new())
}

fn build_row_offsets_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    if settings.has_headers {
        let _ = rdr.byte_headers().map_err(|err| {
            push_warning(warnings, warning_from_error(&err, None));
            err
        })?;
    }

    let mut offsets = Vec::new();
    let mut record = ByteRecord::new();
    let mut row_index: u64 = 0;
    loop {
        let pos = rdr.position().byte();
        match rdr.read_byte_record(&mut record) {
            Ok(false) => break,
            Ok(true) => {
                let mut skip_row = false;
                if let Some(expected) = expected_columns {
                    if record.len() != expected {
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
                if row_index % 10000 == 0 {
                    if let Some(cb) = progress_cb {
                        cb(row_index as usize);
                    }
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

fn read_chunk_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let mut rows = Vec::with_capacity(count);
    let mut record = ByteRecord::new();
    let mut row_index: u64 = 0;
    let mut kept_index: usize = 0;
    let target_end = start + count;

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

                let decoded = match enforce_size_limits(decoded, settings, Some(row_index), warnings)?
                {
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

    let end = usize::min(start + count, offsets.len());
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;
    let mut record = ByteRecord::new();
    let mut rows = Vec::with_capacity(end - start);

    for row_index in start..end {
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

    let mut last_row_index: Option<usize> = None;

    for (row_index, order_idx) in ordered {
        if row_index >= offsets.len() {
            continue;
        }

        if last_row_index.map_or(true, |last| row_index != last + 1) {
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
        }

        let decoded = match apply_length_policy(
            decoded,
            expected_columns,
            settings,
            Some(row_index as u64),
            warnings,
        )? {
            Some(row) => row,
            None => Vec::new(),
        };

        let decoded =
            match enforce_size_limits(decoded, settings, Some(row_index as u64), warnings)? {
                Some(row) => row,
                None => Vec::new(),
            };

        rows[order_idx] = decoded;
        last_row_index = Some(row_index);
    }

    Ok(rows)
}

fn search_range_with_offsets_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query: &str,
    match_case: bool,
    whole_word: bool,
    settings: &ParseSettings,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    if start >= offsets.len() {
        return Ok(Vec::new());
    }

    let end = usize::min(end, offsets.len());
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;

    let mut record = ByteRecord::new();
    let mut matches = Vec::new();
    for row_index in start..end {
        if !rdr.read_byte_record(&mut record)? {
            break;
        }
        let is_match = match column_idx {
            Some(index) => record
                .get(index)
                .and_then(|cell| {
                    let (decoded, _, _) = settings.encoding.decode(cell);
                    let val = decoded.as_ref();
                    let matched = if !match_case {
                         let val_lower = val.to_lowercase();
                         if whole_word {
                             val_lower == query
                         } else {
                             val_lower.contains(query)
                         }
                    } else {
                         if whole_word {
                              val == query
                         } else {
                              val.contains(query)
                         }
                    };
                    Some(matched)
                })
                .unwrap_or(false),
            None => record.iter().any(|cell| {
                let (decoded, _, _) = settings.encoding.decode(cell);
                let val = decoded.as_ref();
                if !match_case {
                     let val_lower = val.to_lowercase();
                     if whole_word {
                         val_lower == query
                     } else {
                         val_lower.contains(query)
                     }
                } else {
                     if whole_word {
                          val == query
                     } else {
                          val.contains(query)
                     }
                }
            }),
        };
        if is_match {
            matches.push(row_index);
        }
    }

    Ok(matches)
}

pub fn build_row_offsets(
    path: &str,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, settings.has_headers);
    build_row_offsets_from_reader(rdr, settings, expected_columns, warnings, progress_cb)
}

pub fn build_row_offsets_mmap(
    data: &[u8],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    build_row_offsets_from_reader(rdr, settings, expected_columns, warnings, progress_cb)
}

pub fn read_chunk(
    path: &str,
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, settings.has_headers);
    read_chunk_from_reader(rdr, start, count, settings, expected_columns, warnings)
}

pub fn read_chunk_mmap(
    data: &[u8],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    read_chunk_from_reader(rdr, start, count, settings, expected_columns, warnings)
}

pub fn read_chunk_with_offsets(
    path: &str,
    offsets: &[u64],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
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
}

pub fn read_chunk_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    count: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
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
}

pub fn read_rows_by_index(
    path: &str,
    offsets: &[u64],
    indices: &[usize],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, false);
    read_rows_by_index_from_reader(
        rdr,
        offsets,
        indices,
        settings,
        expected_columns,
        warnings,
    )
}

pub fn read_rows_by_index_mmap(
    data: &[u8],
    offsets: &[u64],
    indices: &[usize],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(data);
    let rdr = build_reader(cursor, settings, false);
    read_rows_by_index_from_reader(
        rdr,
        offsets,
        indices,
        settings,
        expected_columns,
        warnings,
    )
}

pub fn search_range_with_offsets(
    path: &str,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query: &str,
    match_case: bool,
    whole_word: bool,
    settings: &ParseSettings,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, false);
    search_range_with_offsets_from_reader(
        rdr,
        offsets,
        start,
        end,
        column_idx,
        query,
        match_case,
        whole_word,
        settings,
    )
}

pub fn search_range_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query: &str,
    match_case: bool,
    whole_word: bool,
    settings: &ParseSettings,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(data);
    let rdr = build_reader(cursor, settings, false);
    search_range_with_offsets_from_reader(
        rdr,
        offsets,
        start,
        end,
        column_idx,
        query,
        match_case,
        whole_word,
        settings,
    )
}

pub fn find_duplicates_hashed(
    path: &str,
    offsets: &[u64],
    settings: &ParseSettings,
    column_idx: Option<usize>,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    
    // Force has_headers to false for raw row access
    let mut safe_settings = settings.clone();
    safe_settings.has_headers = false;
    
    let rdr = build_reader(reader, &safe_settings, false);
    
    // 1. Compute Hashes
    let mut hashes = compute_hashes_from_reader(rdr, offsets, column_idx)?;



    // 2. Sort by hash
    hashes.par_sort_unstable_by_key(|k| k.0);



    // 3. Find candidates and verify
    let mut duplicates = Vec::new();
    let mut i = 0;
    while i < hashes.len() {
        let j = i + 1;
        // Find run of identical hashes
        let mut run_end = j;
        while run_end < hashes.len() && hashes[run_end].0 == hashes[i].0 {
            run_end += 1;
        }

        if run_end > i + 1 {


            // Found a collision group of size (run_end - i)
            let candidates: Vec<usize> = hashes[i..run_end].iter().map(|&(_, idx)| idx as usize).collect();
            
            let mut warnings = Vec::new();
            
            let rows = read_rows_by_index(path, offsets, &candidates, &safe_settings, None, &mut warnings)?;
            
            let mut content_map: std::collections::HashMap<Vec<String>, Vec<usize>> = std::collections::HashMap::with_capacity(rows.len());
            
            for (k, row) in rows.into_iter().enumerate() {
                let original_idx = candidates[k];
                let key = match column_idx {
                    Some(idx) => vec![row.get(idx).cloned().unwrap_or_default()],
                    None => row,
                };
                content_map.entry(key).or_default().push(original_idx);
            }
            
            for (_, indices) in content_map {
                if indices.len() > 1 {
                    duplicates.extend(indices);
                }
            }
        }
        
        i = run_end;
    }



    duplicates.sort_unstable();
    Ok(duplicates)
}

pub fn find_duplicates_hashed_mmap(
    data: &[u8],
    offsets: &[u64],
    settings: &ParseSettings,
    column_idx: Option<usize>,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let mut safe_settings = settings.clone();
    safe_settings.has_headers = false;
    
    // 1. Compute Hashes (Parallel)
    // We split into chunks to allow parallel processing.
    // Within each chunk, we assume offsets are sequential (which they are for the whole file),
    // so we can Seek once and Read sequentially for maximum speed.
    // If offsets are NOT sequential (e.g. filtered), this might read wrong data if we just readNext.
    // BUT, find_duplicates logic usually runs on the whole file or filtered set.
    // If filtered, offsets are Monotonic but potentially Sparse.
    // If Sparse, "Read Next" gives the Wrong Row (it gives the immediate physical next).
    // CRITICAL: We MUST check if offsets are contiguous to optimize.
    // Actually, for safety, if we just Seek every time it's slower but correct.
    // OR, we check: if next_offset == current_pos, read. Else seek.
    // given we parse rows, we know how many bytes used? No, ByteRecord doesn't tell us consumed bytes easily?
    // Actually `ByteRecord` + `Position`.
    // Let's stick to "Seek every row" inside the chunk if uncertain, OR "Seek once" if we know we are unfiltered.
    // The `offsets` passed to `find_duplicates` comes from `state.row_offsets`. 
    // This is arguably the WHOLE file offsets.
    // So sequential read is valid.
    
    // We'll use a safer hybrid: In parallel chunk, create reader.
    // For each offset, check if we are at position? No, getting position is slow.
    // Let's assume SEEKING in-memory Cursor is very fast (it is). 
    // `rdr.seek` creates a new internal buffer or clears it. 
    // Optimization: Use `ReaderBuilder` with a decent buffer, but reset is inevitable on seek.
    // Rayon `map` reduces to `Vec<(u64, u32)>`.

    let chunk_size = 4096; // Tunable
    let mut hashes: Vec<(u64, u32)> = offsets.par_chunks(chunk_size)
        .enumerate()
        .map(|(chunk_idx, batch_offsets)| {
            let start_row = chunk_idx * chunk_size;
            let mut local_hashes = Vec::with_capacity(batch_offsets.len());
            // Create a thread-local reader
            let cursor = Cursor::new(data);
             // We reuse settings but has_headers=false for data reading
            let mut rdr = build_reader(cursor, &safe_settings, false);
            let mut record = ByteRecord::new();
            
            // Optimization: If possible, we try to stride.
            // But strict correctness with `seek` for every row is safer given `csv` crate buffering.
            // On memory mapped file, seek is just `cursor.set_position`.
            // The overhead is `rdr` buffer invalidation.
            // For 10M rows, 10M seeks + reads.
            // In parallel (e.g. 8 threads), 1.25M each.
            // Should be fast enough.
            
            for (i, &offset) in batch_offsets.iter().enumerate() {
                let mut pos = Position::new();
                pos.set_byte(offset);
                if rdr.seek(pos).is_ok() {
                    if rdr.read_byte_record(&mut record).unwrap_or(false) {
                         let hash = {
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            if let Some(idx) = column_idx {
                                 if let Some(field) = record.get(idx) {
                                     field.hash(&mut hasher);
                                 }
                            } else {
                                for field in &record {
                                    field.hash(&mut hasher);
                                }
                            }
                            hasher.finish()
                        };
                        local_hashes.push((hash, (start_row + i) as u32));
                    }
                }
            }
            local_hashes
        })
        .flatten()
        .collect();

    // 2. Sort by hash
    hashes.par_sort_unstable_by_key(|k| k.0);

    // 3. Find candidates (Identify Collision Groups)
    // We define a collision group as a range [start, end) where hashes are identical.
    // We can scan linearly to find these ranges (very fast on sorted vec),
    // then process ranges in parallel.
    
    let mut groups = Vec::new();
    let mut i = 0;
    while i < hashes.len() {
        let mut run_end = i + 1;
        while run_end < hashes.len() && hashes[run_end].0 == hashes[i].0 {
            run_end += 1;
        }
        
        if run_end > i + 1 {
            groups.push(i..run_end);
        }
        i = run_end;
    }

    // 4. Verify Groups in Parallel
    let confirmed_duplicates: Vec<usize> = groups.into_par_iter()
        .map(|range| {
            // Re-construct logic for checking rows in this range
            // We need a thread-local reader (or just read bytes slice directly if we knew lengths)
            // But we need CSV parsing for quotes etc.
            // We can just use the `read_rows_by_index_mmap` helper or inline it.
            // Inline is better for avoiding repeated `read_rows` overhead calls (chunking).
            
            // Extract the indices for this group
            let group_indices: Vec<usize> = hashes[range].iter().map(|&(_, idx)| idx as usize).collect();
            
            // Optimization: Since we know the offsets, we can read just those rows.
            // We'll create a local reader.
            
            let cursor = Cursor::new(data);
            let mut rdr = build_reader(cursor, &safe_settings, false);
            let mut record = ByteRecord::new();
            
            // Map Content -> List of Indices
            let mut content_map: std::collections::HashMap<Vec<u8>, Vec<usize>> = std::collections::HashMap::with_capacity(group_indices.len());
            
            for &idx in &group_indices {
                if let Some(&offset) = offsets.get(idx) {
                    let mut pos = Position::new();
                    pos.set_byte(offset);
                    if rdr.seek(pos).is_ok() {
                         if rdr.read_byte_record(&mut record).unwrap_or(false) {
                            // Key is either column or whole row
                            let key = if let Some(c_idx) = column_idx {
                                record.get(c_idx).unwrap_or(&[]).to_vec()
                            } else {
                                // For whole row, we can just use the raw bytes of the record?
                                // ByteRecord is slightly complex structure. `as_slice`? 
                                // `record.as_slice()` is just the field data concatenated? No.
                                // Clone the record into Vec<String>? Expensive.
                                // We can maintain `ByteRecord` -> Vec<Vec<u8>> (fields).
                                // Or just formatted string?
                                // "Duplicate" means Exact Match.
                                // If we assume `ByteRecord` equality implies duplicate.
                                // We can use `record.clone()`? `ByteRecord` is strictly equal if fields equal.
                                // But ByteRecord is not Hashable by default?
                                // It is `Eq`.
                                // Let's use `Vec<u8>` for key.
                                
                                // Actually, `content_map` key.
                                // If we just stick to `Vec<u8>` (bytes of the field).
                                // For whole row, maybe serialize to bytes?
                                
                                let mut k = Vec::new();
                                for field in &record {
                                    k.extend_from_slice(field);
                                    k.push(0); // delimiter-ish to distinguish fields?
                                }
                                k
                                
                            };
                            content_map.entry(key).or_default().push(idx);
                        }
                    }
                }
            }
            
            let mut local_dupes = Vec::new();
            for (_, indices) in content_map {
                if indices.len() > 1 {
                    local_dupes.extend(indices);
                }
            }
            local_dupes
        })
        .flatten()
        .collect();
        
    let mut duplicates = confirmed_duplicates;
    duplicates.sort_unstable();
    Ok(duplicates)
}

fn compute_hashes_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    column_idx: Option<usize>,
) -> Result<Vec<(u64, u32)>, Box<dyn std::error::Error>> {
    let mut hashes = Vec::with_capacity(offsets.len());
    let mut record = ByteRecord::new();
    
    if !offsets.is_empty() {
        let mut pos = Position::new();
        pos.set_byte(offsets[0]);
        rdr.seek(pos)?;
        
        for (i, _) in offsets.iter().enumerate() {
            // We use `read_byte_record` to reuse memory
            if !rdr.read_byte_record(&mut record)? {
                break;
            }
            
            let hash = {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                if let Some(idx) = column_idx {
                     if let Some(field) = record.get(idx) {
                         field.hash(&mut hasher);
                     }
                } else {
                    // Hash all fields
                    for field in &record {
                        field.hash(&mut hasher);
                    }
                }
                hasher.finish()
            };
            
            hashes.push((hash, i as u32));
        }
    }

    Ok(hashes)
}

// Debug helper (appended via command to ensure availability)
// Removed since we can just use eprintln!
