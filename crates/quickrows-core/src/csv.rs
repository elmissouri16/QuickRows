use chardetng::EncodingDetector;
use csv::{ByteRecord, Position, ReaderBuilder, StringRecord, Terminator};
use encoding_rs::Encoding;
use rayon::prelude::*;
use rayon::slice::ParallelSliceMut;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

const SAMPLE_SIZE: usize = 64 * 1024;
pub const MAX_WARNING_COUNT: usize = 200;

pub fn validate_parse_overrides(overrides: &ParseOverrides) -> Result<(), String> {
    for (name, value, valid) in [
        (
            "delimiter",
            overrides.delimiter.as_deref(),
            overrides
                .delimiter
                .as_deref()
                .and_then(normalize_delimiter)
                .is_some(),
        ),
        (
            "quote",
            overrides.quote.as_deref(),
            overrides
                .quote
                .as_deref()
                .and_then(normalize_quote)
                .is_some(),
        ),
        (
            "escape",
            overrides.escape.as_deref(),
            overrides
                .escape
                .as_deref()
                .and_then(normalize_escape)
                .is_some(),
        ),
        (
            "comment",
            overrides.comment.as_deref(),
            overrides
                .comment
                .as_deref()
                .and_then(normalize_comment)
                .is_some(),
        ),
    ] {
        if value.is_some() && !valid {
            return Err(format!(
                "CSV {name} override must be exactly one character or a supported named value"
            ));
        }
    }
    if let Some(line_ending) = overrides.line_ending.as_deref() {
        if normalize_line_ending(line_ending).is_none() {
            return Err(format!("Unsupported CSV line ending: {line_ending}"));
        }
    }
    if let Some(encoding) = overrides.encoding.as_deref() {
        let normalized = normalize_encoding_label(encoding);
        if Encoding::for_label(normalized.as_bytes()).is_none() {
            return Err(format!("Unsupported CSV encoding: {encoding}"));
        }
    }
    if let Some(mode) = overrides.malformed.as_deref() {
        if !matches!(mode, "strict" | "skip" | "repair") {
            return Err(format!("Unsupported malformed-row mode: {mode}"));
        }
    }
    Ok(())
}

pub fn validate_parse_settings(settings: &ParseSettings) -> Result<(), String> {
    let syntax = [
        ("delimiter", Some(settings.delimiter)),
        ("quote", Some(settings.quote)),
        ("escape", settings.escape),
        ("comment", settings.comment),
    ];
    for left in 0..syntax.len() {
        for right in left + 1..syntax.len() {
            if syntax[left].1.is_some() && syntax[left].1 == syntax[right].1 {
                return Err(format!(
                    "CSV {} and {} characters must be different",
                    syntax[left].0, syntax[right].0
                ));
            }
        }
    }
    for (name, value) in [
        ("delimiter", Some(settings.delimiter)),
        ("quote", Some(settings.quote)),
        ("escape", settings.escape),
        ("comment", settings.comment),
    ] {
        if matches!(value, Some('\0' | '\r' | '\n')) {
            return Err(format!(
                "CSV {name} cannot be NUL or a line-ending character"
            ));
        }
        if let Some(value) = value {
            if settings.encoding != encoding_rs::UTF_16LE
                && settings.encoding != encoding_rs::UTF_16BE
                && settings.encoding.encode(&value.to_string()).2
            {
                return Err(format!(
                    "CSV {name} character cannot be represented in {}",
                    settings.encoding_label
                ));
            }
        }
    }
    Ok(())
}

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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ParseOverrides {
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub comment: Option<String>,
    pub excel_sep: Option<bool>,
    pub line_ending: Option<String>,
    pub encoding: Option<String>,
    pub has_headers: Option<bool>,
    pub malformed: Option<String>,
    pub max_field_size: Option<usize>,
    pub max_record_size: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParseInfo {
    pub delimiter: String,
    pub quote: String,
    pub escape: Option<String>,
    pub comment: Option<String>,
    pub excel_sep: bool,
    pub line_ending: String,
    pub encoding: String,
    pub has_headers: bool,
    pub malformed: String,
    pub max_field_size: usize,
    pub max_record_size: usize,
}

#[derive(Clone, Debug)]
pub struct ParseSettings {
    pub delimiter: char,
    pub quote: char,
    pub escape: Option<char>,
    pub comment: Option<char>,
    pub excel_sep: bool,
    pub source_bom: bool,
    pub source_bom_len: usize,
    pub terminator: Terminator,
    pub line_ending: String,
    pub has_headers: bool,
    pub encoding: &'static Encoding,
    pub encoding_label: String,
    pub malformed: MalformedMode,
    pub max_field_size: usize,
    pub max_record_size: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    pub delimiter: char,
    pub quote: char,
    pub escape: Option<char>,
    pub comment: Option<char>,
    pub excel_sep: bool,
    pub source_bom: bool,
    pub source_bom_len: usize,
    pub line_ending: String,
    pub encoding: &'static Encoding,
    pub encoding_label: String,
    pub has_headers: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreservedComment {
    /// Number of canonical CSV records emitted before this comment.
    pub before_record: usize,
    pub text: String,
}

pub struct PreparedCsvSource {
    pub path: PathBuf,
    pub settings: ParseSettings,
    pub temporary: Option<Arc<tempfile::NamedTempFile>>,
    pub warnings: Vec<ParseWarning>,
    pub comments: Vec<PreservedComment>,
}

pub(crate) struct PreparedSavedCsvSource {
    pub prepared: PreparedCsvSource,
    pub headers: Vec<String>,
    pub offsets: Vec<u64>,
    pub raw_len: u64,
    pub raw_content_hash: [u8; 32],
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
        csv::ErrorKind::UnequalLengths {
            expected_len, len, ..
        } => {
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

fn one_scalar(value: &str) -> Option<char> {
    let mut chars = value.chars();
    let value = chars.next()?;
    chars.next().is_none().then_some(value)
}

pub fn normalize_delimiter(value: &str) -> Option<char> {
    if let Some(value) = one_scalar(value) {
        return Some(value);
    }
    let trimmed = value.trim();
    match trimmed.to_lowercase().as_str() {
        "comma" | "," => Some(','),
        "tab" | "\\t" | "tsv" => Some('\t'),
        "semicolon" | ";" => Some(';'),
        "pipe" | "|" => Some('|'),
        "space" => Some(' '),
        _ if value == " " => Some(' '),
        _ => one_scalar(trimmed),
    }
}

pub fn normalize_quote(value: &str) -> Option<char> {
    if let Some(value) = one_scalar(value) {
        return Some(value);
    }
    let trimmed = value.trim();
    match trimmed.to_lowercase().as_str() {
        "double" | "\"" => Some('"'),
        "single" | "'" => Some('\''),
        _ => one_scalar(trimmed),
    }
}

pub fn normalize_escape(value: &str) -> Option<Option<char>> {
    if let Some(value) = one_scalar(value) {
        return Some(Some(value));
    }
    let trimmed = value.trim();
    match trimmed.to_lowercase().as_str() {
        "none" | "off" => Some(None),
        "backslash" | "\\\\" | "\\" => Some(Some('\\')),
        _ => one_scalar(trimmed).map(Some),
    }
}

pub fn normalize_comment(value: &str) -> Option<Option<char>> {
    if let Some(value) = one_scalar(value) {
        return Some(Some(value));
    }
    let trimmed = value.trim();
    match trimmed.to_lowercase().as_str() {
        "none" | "off" => Some(None),
        "hash" | "#" => Some(Some('#')),
        _ => one_scalar(trimmed).map(Some),
    }
}

fn normalize_encoding_label(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "latin1" | "latin-1" => "iso-8859-1".to_string(),
        "utf8" => "utf-8".to_string(),
        other => other.to_string(),
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

fn detect_line_ending(sample: &str, quote: char, escape: Option<char>) -> String {
    let mut crlf = 0;
    let mut lf = 0;
    let mut cr = 0;
    let mut in_quotes = false;
    let mut chars = sample.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_quotes && escape == Some(ch) && chars.peek() == Some(&quote) {
            chars.next();
        } else if ch == quote {
            if in_quotes && chars.peek() == Some(&quote) {
                chars.next();
            } else {
                in_quotes = !in_quotes;
            }
        } else if ch == '\r' && !in_quotes {
            if chars.peek() == Some(&'\n') {
                chars.next();
                crlf += 1;
            } else {
                cr += 1;
            }
        } else if ch == '\n' && !in_quotes {
            lf += 1;
        }
    }
    if crlf >= lf && crlf >= cr {
        "crlf".to_string()
    } else if lf >= cr {
        "lf".to_string()
    } else {
        "cr".to_string()
    }
}

fn quote_syntax_score(sample: &str, quote: char) -> usize {
    sample
        .split(['\r', '\n'])
        .filter(|line| {
            let trimmed = line.trim();
            let starts_quoted =
                trimmed.starts_with(quote) && trimmed[quote.len_utf8()..].contains(quote);
            let follows_delimiter = [',', ';', '\t', '|', ':']
                .iter()
                .any(|delimiter| line.contains(&format!("{delimiter}{quote}")));
            starts_quoted || follows_delimiter
        })
        .count()
}

fn detect_quote_char(sample: &str) -> char {
    let double_score = quote_syntax_score(sample, '"');
    let single_score = quote_syntax_score(sample, '\'');
    if single_score > double_score {
        '\''
    } else {
        '"'
    }
}

fn parse_detection_record(line: &str, delimiter: char, quote: char) -> Option<StringRecord> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        if ch == quote {
            if in_quotes && chars.peek() == Some(&quote) {
                chars.next();
                field.push(quote);
            } else {
                in_quotes = !in_quotes;
            }
        } else if ch == delimiter && !in_quotes {
            fields.push(std::mem::take(&mut field));
        } else {
            field.push(ch);
        }
    }
    if in_quotes {
        return None;
    }
    fields.push(field);
    Some(StringRecord::from(fields))
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

fn detect_delimiter(sample: &str, quote: char) -> char {
    let quote_char = quote;
    let mut candidates = vec![',', '\t', ';', '|', ':', '^', '~', '\u{1f}'];
    let common_candidate_count = candidates.len();
    for candidate in sample.chars().filter(|candidate| {
        !candidate.is_ascii()
            && !candidate.is_alphanumeric()
            && !matches!(candidate, '\r' | '\n' | ' ' | '"' | '\'' | '\\')
    }) {
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
    let lines = sample
        .split(['\r', '\n'])
        .filter(|line| !line.trim().is_empty())
        .take(20)
        .collect::<Vec<_>>();
    let mut best = (',', 0usize, 0usize);

    for (candidate_index, candidate) in candidates.into_iter().enumerate() {
        let counts: Vec<usize> = if candidate.is_ascii() && quote_char.is_ascii() {
            let mut builder = ReaderBuilder::new();
            builder
                .has_headers(false)
                .delimiter(candidate as u8)
                .quote(quote_char as u8)
                .terminator(Terminator::CRLF)
                .flexible(true);
            builder
                .from_reader(sample.as_bytes())
                .records()
                .take(20)
                .filter_map(Result::ok)
                .map(|record| record.len())
                .collect()
        } else {
            lines
                .iter()
                .filter_map(|line| count_fields(line, candidate, quote_char))
                .collect()
        };
        if counts.is_empty() || (counts.len() < 2 && candidate_index >= common_candidate_count) {
            continue;
        }
        let first_count = counts[0];
        let parsed_count = counts.len();
        let mut freq = std::collections::HashMap::<usize, usize>::new();
        for &count in &counts {
            *freq.entry(count).or_insert(0) += 1;
        }
        if let Some((mode_count, mode_freq)) =
            freq.into_iter().max_by_key(|(count, freq)| (*freq, *count))
        {
            let minimum_consistent =
                if candidate_index < common_candidate_count && parsed_count == 1 {
                    1
                } else {
                    2
                };
            let consistently_rectangular = first_count == mode_count
                && mode_freq >= parsed_count.saturating_sub(1).max(minimum_consistent);
            if mode_count > 1
                && consistently_rectangular
                && (mode_freq > best.2 || (mode_freq == best.2 && mode_count > best.1))
            {
                best = (candidate, mode_count, mode_freq);
            }
        }
    }

    best.0
}

fn detect_escape(sample: &str, quote: char) -> Option<char> {
    let needle = format!("\\{quote}");
    if sample.contains(&needle) {
        Some('\\')
    } else {
        None
    }
}

fn detect_excel_sep(sample: &str) -> Option<(char, usize)> {
    let line_end = sample.find(['\r', '\n']).unwrap_or(sample.len());
    let first = sample[..line_end].trim_end();
    let value = first.strip_prefix("sep=")?;
    let delimiter = one_scalar(value)?;
    let mut consumed = line_end;
    let suffix = &sample[line_end..];
    if suffix.starts_with("\r\n") {
        consumed += 2;
    } else if suffix.starts_with(['\r', '\n']) {
        consumed += 1;
    }
    Some((delimiter, consumed))
}

fn read_sample(path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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

    false
}

pub fn detect_parse_settings(
    path: impl AsRef<Path>,
) -> Result<DetectedSettings, Box<dyn std::error::Error>> {
    detect_parse_settings_for_encoding(path, None)
}

pub fn detect_parse_settings_for_encoding(
    path: impl AsRef<Path>,
    encoding_override: Option<&str>,
) -> Result<DetectedSettings, Box<dyn std::error::Error>> {
    let sample = read_sample(path.as_ref())?;
    let (detected_encoding, detected_label, bom_len) = detect_encoding(&sample);
    let forced_encoding = encoding_override
        .map(normalize_encoding_label)
        .and_then(|label| Encoding::for_label(label.as_bytes()));
    let encoding = forced_encoding.unwrap_or(detected_encoding);
    let encoding_label = forced_encoding
        .map(|encoding| encoding.name().to_string())
        .unwrap_or(detected_label);
    let sample_no_bom = sample.get(bom_len..).unwrap_or(&sample);
    let (decoded, _, _) = encoding.decode(sample_no_bom);
    let decoded = decoded.into_owned();

    let excel_sep = detect_excel_sep(&decoded);
    let detection_sample = excel_sep
        .and_then(|(_, consumed)| decoded.get(consumed..))
        .unwrap_or(&decoded);
    let quote = detect_quote_char(detection_sample);
    let escape = detect_escape(detection_sample, quote);
    let line_ending = detect_line_ending(detection_sample, quote, escape);
    let delimiter = excel_sep
        .map(|(delimiter, _)| delimiter)
        .unwrap_or_else(|| detect_delimiter(detection_sample, quote));

    let has_headers = if delimiter.is_ascii()
        && quote.is_ascii()
        && escape.is_none_or(|value| value.is_ascii())
    {
        let mut builder = ReaderBuilder::new();
        builder
            .has_headers(false)
            .delimiter(delimiter as u8)
            .quote(quote as u8)
            .escape(escape.map(|value| value as u8))
            .terminator(Terminator::CRLF)
            .flexible(true);
        let mut rdr = builder.from_reader(detection_sample.as_bytes());
        let mut rows = rdr.records();
        let first = rows.next().and_then(|row| row.ok());
        let second = rows.next().and_then(|row| row.ok());
        match (first, second) {
            (Some(first), Some(second)) => looks_like_header(&first, &second),
            _ => false,
        }
    } else {
        let mut rows = detection_sample
            .split(['\r', '\n'])
            .filter(|line| !line.is_empty())
            .filter_map(|line| parse_detection_record(line, delimiter, quote));
        match (rows.next(), rows.next()) {
            (Some(first), Some(second)) => looks_like_header(&first, &second),
            _ => false,
        }
    };

    Ok(DetectedSettings {
        delimiter,
        quote,
        escape,
        comment: None,
        excel_sep: excel_sep.is_some(),
        source_bom: bom_len > 0 && encoding == detected_encoding,
        source_bom_len: bom_len,
        line_ending,
        encoding,
        encoding_label,
        has_headers,
    })
}

fn format_char(value: char) -> String {
    value.to_string()
}

pub fn parse_info_from_settings(settings: &ParseSettings) -> ParseInfo {
    ParseInfo {
        delimiter: format_char(settings.delimiter),
        quote: format_char(settings.quote),
        escape: settings.escape.map(format_char),
        comment: settings.comment.map(format_char),
        excel_sep: settings.excel_sep,
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
    let mut comment = detected.comment;
    let mut excel_sep = detected.excel_sep;
    let source_bom_len = detected.source_bom_len;
    let mut terminator = Terminator::CRLF;
    let mut line_ending = detected.line_ending.clone();
    let mut encoding = detected.encoding;
    let mut encoding_label = detected.encoding_label.clone();
    let mut has_headers = detected.has_headers;
    let mut malformed = MalformedMode::Skip;
    let mut max_field_size = usize::MAX;
    let mut max_record_size = usize::MAX;

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
        if let Some(value) = overrides.comment.as_deref() {
            if let Some(parsed) = normalize_comment(value) {
                comment = parsed;
            }
        }
        if let Some(value) = overrides.excel_sep {
            excel_sep = value && detected.excel_sep;
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
            let normalized = normalize_encoding_label(value);
            if let Some(enc) = Encoding::for_label(normalized.as_bytes()) {
                encoding = enc;
                encoding_label = enc.name().to_ascii_lowercase();
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

    let source_bom = detected.source_bom && encoding == detected.encoding;

    ParseSettings {
        delimiter,
        quote,
        escape,
        comment,
        excel_sep,
        source_bom,
        source_bom_len,
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
        delimiter: ',',
        quote: '"',
        escape: None,
        comment: None,
        excel_sep: false,
        source_bom: false,
        source_bom_len: 0,
        terminator: Terminator::CRLF,
        line_ending: "auto".to_string(),
        has_headers: true,
        encoding: encoding_rs::UTF_8,
        encoding_label: "utf-8".to_string(),
        malformed: MalformedMode::Skip,
        max_field_size: usize::MAX,
        max_record_size: usize::MAX,
    }
}

pub(crate) fn canonical_storage_settings(settings: &ParseSettings) -> ParseSettings {
    let mut storage_settings = settings.clone();
    storage_settings.delimiter = ',';
    storage_settings.quote = '"';
    storage_settings.escape = None;
    storage_settings.comment = None;
    storage_settings.excel_sep = false;
    storage_settings.source_bom = false;
    storage_settings.source_bom_len = 0;
    storage_settings.terminator = Terminator::Any(b'\n');
    storage_settings.line_ending = "lf".to_string();
    storage_settings.encoding = encoding_rs::UTF_8;
    storage_settings.encoding_label = "utf-8".to_string();
    storage_settings
}

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

    fn write_record(&mut self, fields: &[String]) -> Result<(), String>;
    fn finish(self) -> Result<Self::Output, String>;
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

    fn write_record(&mut self, fields: &[String]) -> Result<(), String> {
        self.writer
            .write_record(fields)
            .map_err(|error| error.to_string())
    }

    fn finish(mut self) -> Result<Self::Output, String> {
        self.writer.flush().map_err(|error| error.to_string())
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

    fn write_canonical_record(&mut self, fields: &[String]) -> Result<(), String> {
        self.record_writer
            .write_record(fields)
            .map_err(|error| error.to_string())?;
        self.record_writer
            .flush()
            .map_err(|error| error.to_string())?;
        let mut bytes = self.record_bytes.borrow_mut();
        self.output
            .write_all(&bytes)
            .map_err(|error| error.to_string())?;
        self.position = self.position.saturating_add(bytes.len() as u64);
        bytes.clear();
        Ok(())
    }
}

impl<W: Write> CanonicalRecordSink for IndexedSavedCsvSink<W> {
    type Output = SavedCanonicalOutput;

    fn write_record(&mut self, fields: &[String]) -> Result<(), String> {
        let is_header = self.has_headers && self.records_seen == 0;
        if is_header {
            if fields != self.expected_headers {
                return Err("Saved CSV headers changed during validation".to_string());
            }
            self.headers = fields.to_vec();
        } else {
            if !self.has_headers && self.headers.is_empty() {
                self.headers = (0..fields.len())
                    .map(|column| format!("Column {}", column + 1))
                    .collect();
                if !self.expected_headers.is_empty() && self.headers != self.expected_headers {
                    return Err("Saved CSV column count changed during validation".to_string());
                }
            }
            if fields.len() != self.headers.len() {
                return Err(format!(
                    "Saved CSV row has {} fields, expected {}",
                    fields.len(),
                    self.headers.len()
                ));
            }
            self.offsets.push(self.position);
        }
        self.write_canonical_record(fields)?;
        self.records_seen = self.records_seen.saturating_add(1);
        Ok(())
    }

    fn finish(mut self) -> Result<Self::Output, String> {
        if self.has_headers && !self.expected_headers.is_empty() && self.records_seen == 0 {
            return Err("Saved CSV is missing its header record".to_string());
        }
        if self.offsets.len() != self.expected_rows {
            return Err(format!(
                "Saved CSV has {} rows, expected {}",
                self.offsets.len(),
                self.expected_rows
            ));
        }
        self.output.flush().map_err(|error| error.to_string())?;
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

    fn malformed(&mut self, message: &str, kind: &str) -> Result<(), String> {
        if self.settings.malformed == MalformedMode::Strict {
            return Err(message.to_string());
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
    ) -> Result<(), String> {
        if self.settings.malformed == MalformedMode::Strict {
            return Err(message);
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

    fn push_field_char(&mut self, ch: char) -> Result<(), String> {
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

    fn finish_record(&mut self) -> Result<(), String> {
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

    fn process_char(&mut self, ch: char) -> Result<(), String> {
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

    fn feed(&mut self, text: &str) -> Result<(), String> {
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

    fn finish(mut self) -> Result<S::Output, String> {
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

fn stream_canonical_csv_to_sink<S: CanonicalRecordSink>(
    path: &Path,
    settings: &ParseSettings,
    sink: S,
    warnings: &mut Vec<ParseWarning>,
    comments: &mut Vec<PreservedComment>,
    progress: Option<&dyn Fn(usize)>,
    is_cancelled: &dyn Fn() -> bool,
    mut raw_fingerprint: Option<&mut RawStreamFingerprint>,
) -> Result<S::Output, String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    if settings.source_bom_len > 0 {
        let mut bom = vec![0u8; settings.source_bom_len];
        file.read_exact(&mut bom)
            .map_err(|error| error.to_string())?;
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
            return Err("Operation cancelled".to_string());
        }
        let read = reader.read(&mut input).map_err(|error| error.to_string())?;
        if let Some(fingerprint) = raw_fingerprint.as_deref_mut() {
            fingerprint.update(&input[..read]);
        }
        total_read += read;
        let last = read == 0;
        let mut consumed = 0;
        loop {
            if is_cancelled() {
                return Err("Operation cancelled".to_string());
            }
            let (result, used, written) = decoder.decode_to_utf8_without_replacement(
                &input[consumed..read],
                &mut decoded,
                last,
            );
            consumed += used;
            let text = std::str::from_utf8(&decoded[..written])
                .map_err(|_| "Decoder produced invalid UTF-8".to_string())?;
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
) -> Result<(), String> {
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

fn csv_tempfile_near(path: &Path, prefix: &str) -> Result<tempfile::NamedTempFile, String> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix).suffix(".csv");
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        if let Ok(temporary) = builder.tempfile_in(parent) {
            return Ok(temporary);
        }
    }
    builder.tempfile().map_err(|error| error.to_string())
}

pub fn prepare_csv_source(
    path: &Path,
    settings: &ParseSettings,
) -> Result<PreparedCsvSource, String> {
    prepare_csv_source_cancellable(path, settings, None, &|| false)
}

pub fn prepare_csv_source_cancellable(
    path: &Path,
    settings: &ParseSettings,
    progress: Option<&dyn Fn(usize)>,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<PreparedCsvSource, String> {
    if is_cancelled() {
        return Err("Operation cancelled".to_string());
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
        .map_err(|error| error.to_string())?;

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
) -> Result<PreparedSavedCsvSource, String> {
    if strict_settings.malformed != MalformedMode::Strict {
        return Err("Saved CSV validation requires strict malformed-row handling".to_string());
    }
    if is_cancelled() {
        return Err("Operation cancelled".to_string());
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
        return Err("Saved CSV produced warnings during strict validation".to_string());
    }
    if is_cancelled() {
        return Err("Operation cancelled".to_string());
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
                        repaired.extend(
                            std::iter::repeat(String::new()).take(expected - repaired.len()),
                        );
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
    }
    Ok(Some(fields))
}

pub fn detect_headers_for_settings(
    path: impl AsRef<Path>,
    settings: &ParseSettings,
) -> Result<bool, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = build_reader(file, settings, false);
    let mut rows = reader.byte_records();
    let first = rows.next().transpose()?;
    let second = rows.next().transpose()?;
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
    if rdr.read_byte_record(&mut record).map_err(|err| {
        push_warning(warnings, warning_from_error(&err, None));
        err
    })? {
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
    cancel_cb: Option<&dyn Fn() -> bool>,
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

    for row_index in start..end {
        if rdr.position().byte() != offsets[row_index] {
            let mut position = Position::new();
            position.set_byte(offsets[row_index]);
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

    for row_index in start..end {
        if rdr.position().byte() != offsets[row_index] {
            let mut position = Position::new();
            position.set_byte(offsets[row_index]);
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
    let normalized_query = (!match_case).then(|| query.to_lowercase());
    let query = normalized_query.as_deref().unwrap_or(query);

    let mut record = ByteRecord::new();
    let mut matches = Vec::new();
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;
    for row_index in start..end {
        if rdr.position().byte() != offsets[row_index] {
            let mut position = Position::new();
            position.set_byte(offsets[row_index]);
            rdr.seek(position)?;
        }
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
                    } else if whole_word {
                        val == query
                    } else {
                        val.contains(query)
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
                } else if whole_word {
                    val == query
                } else {
                    val.contains(query)
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
    path: impl AsRef<Path>,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, settings.has_headers);
    build_row_offsets_from_reader(rdr, settings, expected_columns, warnings, progress_cb, None)
}

pub fn build_row_offsets_cancellable(
    path: impl AsRef<Path>,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
    cancel_cb: &dyn Fn() -> bool,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
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
}

pub fn build_row_offsets_mmap(
    data: &[u8],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    build_row_offsets_from_reader(rdr, settings, expected_columns, warnings, progress_cb, None)
}

pub fn build_row_offsets_mmap_cancellable(
    data: &[u8],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
    progress_cb: Option<&dyn Fn(usize)>,
    cancel_cb: &dyn Fn() -> bool,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let rdr = build_reader(data, settings, settings.has_headers);
    build_row_offsets_from_reader(
        rdr,
        settings,
        expected_columns,
        warnings,
        progress_cb,
        Some(cancel_cb),
    )
}

pub fn read_chunk(
    path: impl AsRef<Path>,
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
    path: impl AsRef<Path>,
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

pub(crate) fn read_column_range_with_offsets(
    path: impl AsRef<Path>,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Option<String>>, Box<dyn std::error::Error>> {
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
}

pub(crate) fn read_column_range_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: usize,
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Option<String>>, Box<dyn std::error::Error>> {
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
}

pub fn read_rows_by_index(
    path: impl AsRef<Path>,
    offsets: &[u64],
    indices: &[usize],
    settings: &ParseSettings,
    expected_columns: Option<usize>,
    warnings: &mut Vec<ParseWarning>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = build_reader(reader, settings, false);
    read_rows_by_index_from_reader(rdr, offsets, indices, settings, expected_columns, warnings)
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
    read_rows_by_index_from_reader(rdr, offsets, indices, settings, expected_columns, warnings)
}

pub fn search_range_with_offsets(
    path: impl AsRef<Path>,
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
        rdr, offsets, start, end, column_idx, query, match_case, whole_word, settings,
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
        rdr, offsets, start, end, column_idx, query, match_case, whole_word, settings,
    )
}

pub fn find_duplicates_hashed(
    path: impl AsRef<Path>,
    offsets: &[u64],
    settings: &ParseSettings,
    column_idx: Option<usize>,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let path = path.as_ref();
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
            let candidates: Vec<usize> = hashes[i..run_end].iter().map(|&(_, idx)| idx).collect();

            let mut warnings = Vec::new();

            let rows = read_rows_by_index(
                path,
                offsets,
                &candidates,
                &safe_settings,
                None,
                &mut warnings,
            )?;

            let mut content_map: std::collections::HashMap<Vec<String>, Vec<usize>> =
                std::collections::HashMap::with_capacity(rows.len());

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
    // Rayon `map` reduces to `Vec<(u64, usize)>`.

    let chunk_size = 4096; // Tunable
    let mut hashes: Vec<(u64, usize)> = offsets
        .par_chunks(chunk_size)
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
                        local_hashes.push((hash, start_row + i));
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
    let confirmed_duplicates: Vec<usize> = groups
        .into_par_iter()
        .map(|range| {
            // Re-construct logic for checking rows in this range
            // We need a thread-local reader (or just read bytes slice directly if we knew lengths)
            // But we need CSV parsing for quotes etc.
            // We can just use the `read_rows_by_index_mmap` helper or inline it.
            // Inline is better for avoiding repeated `read_rows` overhead calls (chunking).

            // Extract the indices for this group
            let group_indices: Vec<usize> = hashes[range].iter().map(|&(_, idx)| idx).collect();

            // Optimization: Since we know the offsets, we can read just those rows.
            // We'll create a local reader.

            let cursor = Cursor::new(data);
            let mut rdr = build_reader(cursor, &safe_settings, false);
            let mut record = ByteRecord::new();

            // Map Content -> List of Indices
            let mut content_map: std::collections::HashMap<Vec<u8>, Vec<usize>> =
                std::collections::HashMap::with_capacity(group_indices.len());

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
) -> Result<Vec<(u64, usize)>, Box<dyn std::error::Error>> {
    let mut hashes = Vec::with_capacity(offsets.len());
    let mut record = ByteRecord::new();

    if !offsets.is_empty() {
        for (i, offset) in offsets.iter().copied().enumerate() {
            if rdr.position().byte() != offset {
                let mut pos = Position::new();
                pos.set_byte(offset);
                rdr.seek(pos)?;
            }
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

            hashes.push((hash, i));
        }
    }

    Ok(hashes)
}

// Debug helper (appended via command to ensure availability)
// Removed since we can just use eprintln!

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{SeekFrom, Write};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
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
    fn finite_size_limits_stream_oversized_records_into_a_bounded_snapshot() {
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

        assert!(prepared.temporary.is_some());
        assert!(std::fs::metadata(&prepared.path).unwrap().len() < 128);
        assert_eq!(
            std::fs::read_to_string(&prepared.path).unwrap(),
            "name,value\n"
        );
        assert!(prepared
            .warnings
            .iter()
            .any(|warning| warning.kind == "max-field-size"));
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
        let offsets =
            build_row_offsets(file.path(), &settings, Some(2), &mut warnings, None).unwrap();
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
    fn saved_preparation_writes_canonical_offsets_during_validation() {
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
        let generic = prepare_csv_source(file.path(), &settings).expect("prepare generic source");
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
        assert_eq!(
            std::fs::read(&saved.prepared.path).unwrap(),
            std::fs::read(&generic.path).unwrap()
        );
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
        let file =
            write_temp_csv("id,name\r\n1,Alice\r\n2,Bob\r\n1,Alice\r\n3,Charlie\r\n2,Bob\r\n");
        let settings = default_parse_settings();
        let mut warnings = Vec::new();
        let offsets = build_row_offsets(file.path(), &settings, Some(2), &mut warnings, None)
            .expect("build offsets");

        let duplicates = find_duplicates_hashed(file.path(), &offsets, &settings, None)
            .expect("find duplicates");
        assert_eq!(duplicates, vec![0, 1, 2, 4]);

        let data = std::fs::read(file.path()).expect("read file");
        let mmap_duplicates =
            find_duplicates_hashed_mmap(&data, &offsets, &settings, None).expect("find mmap");
        assert_eq!(mmap_duplicates, vec![0, 1, 2, 4]);
    }
}
