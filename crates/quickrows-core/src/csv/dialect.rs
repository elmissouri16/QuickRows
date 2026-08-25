pub fn validate_parse_overrides(overrides: &ParseOverrides) -> QuickRowsResult<()> {
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
            return Err(QuickRowsError::invalid_settings(format!(
                "CSV {name} override must be exactly one character or a supported named value"
            )));
        }
    }
    if let Some(line_ending) = overrides.line_ending.as_deref()
        && normalize_line_ending(line_ending).is_none()
    {
        return Err(QuickRowsError::invalid_settings(format!(
            "Unsupported CSV line ending: {line_ending}"
        )));
    }
    if let Some(encoding) = overrides.encoding.as_deref() {
        let normalized = normalize_encoding_label(encoding);
        if Encoding::for_label(normalized.as_bytes()).is_none() {
            return Err(QuickRowsError::invalid_settings(format!(
                "Unsupported CSV encoding: {encoding}"
            )));
        }
    }
    if let Some(mode) = overrides.malformed.as_deref()
        && !matches!(mode, "strict" | "skip" | "repair")
    {
        return Err(QuickRowsError::invalid_settings(format!(
            "Unsupported malformed-row mode: {mode}"
        )));
    }
    Ok(())
}

/// Validates overrides and the effective syntax they produce against detected
/// parse information. UI clients should call this instead of duplicating CSV
/// character-conflict rules.
pub fn validate_parse_overrides_for_info(
    overrides: &ParseOverrides,
    detected: Option<&ParseInfo>,
) -> QuickRowsResult<()> {
    validate_parse_overrides(overrides)?;
    let delimiter = overrides
        .delimiter
        .as_deref()
        .and_then(normalize_delimiter)
        .or_else(|| detected.and_then(|info| info.delimiter.chars().next()))
        .unwrap_or(',');
    let quote = overrides
        .quote
        .as_deref()
        .and_then(normalize_quote)
        .or_else(|| detected.and_then(|info| info.quote.chars().next()))
        .unwrap_or('"');
    let escape = match overrides.escape.as_deref() {
        Some(value) => normalize_escape(value).flatten(),
        None => detected.and_then(|info| info.escape.as_deref()?.chars().next()),
    };
    let comment = match overrides.comment.as_deref() {
        Some(value) => normalize_comment(value).flatten(),
        None => detected.and_then(|info| info.comment.as_deref()?.chars().next()),
    };
    validate_syntax_characters(delimiter, quote, escape, comment)
}

fn validate_syntax_characters(
    delimiter: char,
    quote: char,
    escape: Option<char>,
    comment: Option<char>,
) -> QuickRowsResult<()> {
    let syntax = [
        ("delimiter", Some(delimiter)),
        ("quote", Some(quote)),
        ("escape", escape),
        ("comment", comment),
    ];
    for left in 0..syntax.len() {
        for right in left + 1..syntax.len() {
            if syntax[left].1.is_some() && syntax[left].1 == syntax[right].1 {
                return Err(QuickRowsError::invalid_settings(format!(
                    "CSV {} and {} characters must be different",
                    syntax[left].0, syntax[right].0
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_parse_settings(settings: &ParseSettings) -> QuickRowsResult<()> {
    validate_syntax_characters(
        settings.delimiter,
        settings.quote,
        settings.escape,
        settings.comment,
    )?;
    for (name, value) in [
        ("delimiter", Some(settings.delimiter)),
        ("quote", Some(settings.quote)),
        ("escape", settings.escape),
        ("comment", settings.comment),
    ] {
        if matches!(value, Some('\0' | '\r' | '\n')) {
            return Err(QuickRowsError::invalid_settings(format!(
                "CSV {name} cannot be NUL or a line-ending character"
            )));
        }
        if let Some(value) = value
            && settings.encoding != encoding_rs::UTF_16LE
            && settings.encoding != encoding_rs::UTF_16BE
            && settings.encoding.encode(&value.to_string()).2
        {
            return Err(QuickRowsError::invalid_settings(format!(
                "CSV {name} character cannot be represented in {}",
                settings.encoding_label
            )));
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
    if in_quotes { None } else { Some(count) }
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

pub fn detect_parse_settings(path: impl AsRef<Path>) -> QuickRowsResult<DetectedSettings> {
    detect_parse_settings_for_encoding(path, None)
}

pub fn detect_parse_settings_for_encoding(
    path: impl AsRef<Path>,
    encoding_override: Option<&str>,
) -> QuickRowsResult<DetectedSettings> {
    let sample = read_sample(path.as_ref()).map_err(QuickRowsError::from)?;
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
        if let Some(value) = overrides.delimiter.as_deref()
            && let Some(parsed) = normalize_delimiter(value)
        {
            delimiter = parsed;
        }
        if let Some(value) = overrides.quote.as_deref()
            && let Some(parsed) = normalize_quote(value)
        {
            quote = parsed;
        }
        if let Some(value) = overrides.escape.as_deref()
            && let Some(parsed) = normalize_escape(value)
        {
            escape = parsed;
        }
        if let Some(value) = overrides.comment.as_deref()
            && let Some(parsed) = normalize_comment(value)
        {
            comment = parsed;
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
