fn next_document_generation() -> u64 {
    NEXT_DOCUMENT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

struct FingerprintingWriter<W: Write> {
    inner: W,
    hasher: blake3::Hasher,
    len: u64,
}

impl<W: Write> FingerprintingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
            len: 0,
        }
    }

    fn finish(mut self) -> QuickRowsResult<(W, u64, [u8; 32])> {
        self.flush().map_err(QuickRowsError::from)?;
        let hash = *self.hasher.finalize().as_bytes();
        Ok((self.inner, self.len, hash))
    }
}

impl<W: Write> Write for FingerprintingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.len = self.len.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn line_ending(settings: &ParseSettings) -> &'static str {
    match settings.line_ending.as_str() {
        "crlf" => "\r\n",
        "cr" => "\r",
        _ => "\n",
    }
}

fn push_csv_record(output: &mut String, fields: &[String], settings: &ParseSettings) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            output.push(settings.delimiter);
        }
        let needs_quote = field.contains(settings.delimiter)
            || field.contains(settings.quote)
            || field.contains(['\r', '\n'])
            || (index == 0
                && settings
                    .comment
                    .is_some_and(|comment| field.starts_with(comment)));
        if !needs_quote {
            output.push_str(field);
            continue;
        }
        output.push(settings.quote);
        for ch in field.chars() {
            if ch == settings.quote {
                if let Some(escape) = settings.escape {
                    output.push(escape);
                    output.push(ch);
                } else {
                    output.push(ch);
                    output.push(ch);
                }
            } else {
                output.push(ch);
            }
        }
        output.push(settings.quote);
    }
}

fn encode_csv_text_into(
    text: &str,
    settings: &ParseSettings,
    output: &mut Vec<u8>,
) -> QuickRowsResult<()> {
    output.clear();
    if settings.encoding == encoding_rs::UTF_16LE || settings.encoding == encoding_rs::UTF_16BE {
        output.reserve(text.len().saturating_mul(2).saturating_add(2));
        if settings.source_bom {
            if settings.encoding == encoding_rs::UTF_16LE {
                output.extend_from_slice(&[0xff, 0xfe]);
            } else {
                output.extend_from_slice(&[0xfe, 0xff]);
            }
        }
        for unit in text.encode_utf16() {
            let bytes = if settings.encoding == encoding_rs::UTF_16LE {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            output.extend_from_slice(&bytes);
        }
        return Ok(());
    }

    let (encoded, _, had_errors) = settings.encoding.encode(text);
    if had_errors {
        return Err(QuickRowsError::invalid_csv(format!(
            "A value cannot be represented in {}. Change the output encoding or edit the value.",
            settings.encoding_label
        )));
    }
    output.reserve(encoded.len().saturating_add(3));
    if settings.source_bom && settings.encoding == encoding_rs::UTF_8 {
        output.extend_from_slice(&[0xef, 0xbb, 0xbf]);
    }
    output.extend_from_slice(encoded.as_ref());
    Ok(())
}

fn write_encoded_csv_text<W: Write + ?Sized>(
    output: &mut W,
    text: &str,
    settings: &ParseSettings,
    encoded: &mut Vec<u8>,
) -> QuickRowsResult<()> {
    if settings.encoding == encoding_rs::UTF_8 {
        if settings.source_bom {
            output
                .write_all(&[0xef, 0xbb, 0xbf])
                .map_err(QuickRowsError::from)?;
        }
        return output
            .write_all(text.as_bytes())
            .map_err(QuickRowsError::from);
    }

    encode_csv_text_into(text, settings, encoded)?;
    output.write_all(encoded).map_err(QuickRowsError::from)
}

fn write_csv_record<W: Write + ?Sized>(
    output: &mut W,
    fields: &[String],
    csv_settings: &ParseSettings,
    encoding_settings: &ParseSettings,
    terminator: &str,
    record: &mut String,
    encoded: &mut Vec<u8>,
) -> QuickRowsResult<()> {
    record.clear();
    push_csv_record(record, fields, csv_settings);
    record.push_str(terminator);
    write_encoded_csv_text(output, record, encoding_settings, encoded)
}

#[derive(Debug)]
enum RowPostings {
    One(usize),
    Many(Vec<usize>),
}

impl RowPostings {
    fn push(&mut self, row: usize) {
        match self {
            Self::One(first) => {
                let first = *first;
                *self = Self::Many(vec![first, row]);
            }
            Self::Many(rows) => rows.push(row),
        }
    }

    fn as_slice(&self) -> &[usize] {
        match self {
            Self::One(row) => std::slice::from_ref(row),
            Self::Many(rows) => rows,
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn to_vec(&self) -> Vec<usize> {
        self.as_slice().to_vec()
    }

    fn shrink_to_fit(&mut self) {
        if let Self::Many(rows) = self {
            rows.shrink_to_fit();
        }
    }
}

type ColumnSearchIndex = HashMap<String, RowPostings>;

fn index_value(index: &mut ColumnSearchIndex, value: String, source_row: usize) {
    match index.entry(value) {
        std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().push(source_row),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(RowPostings::One(source_row));
        }
    }
}

fn compact_index(index: &mut ColumnSearchIndex) {
    for postings in index.values_mut() {
        postings.shrink_to_fit();
    }
    index.shrink_to_fit();
}

fn sort_projected_order<C, O>(
    values: &[String],
    ascending: bool,
    chunk_size: usize,
    check_cancellation: &C,
    observe_comparison: &O,
) -> QuickRowsResult<Vec<usize>>
where
    C: Fn() -> QuickRowsResult<()> + Sync,
    O: Fn() + Sync,
{
    debug_assert!(chunk_size > 0);
    let compare_rows = |left: &usize, right: &usize| {
        observe_comparison();
        let value_order = if ascending {
            values[*left].cmp(&values[*right])
        } else {
            values[*right].cmp(&values[*left])
        };
        value_order.then_with(|| left.cmp(right))
    };
    let mut order = (0..values.len()).collect::<Vec<_>>();
    order
        .par_chunks_mut(chunk_size)
        .try_for_each(|chunk| -> QuickRowsResult<()> {
            check_cancellation()?;
            chunk.sort_unstable_by(&compare_rows);
            check_cancellation()
        })?;

    let mut scratch = vec![0usize; order.len()];
    let mut source_is_order = true;
    let mut width = chunk_size;
    while width < order.len() {
        check_cancellation()?;
        let pair_width = width.saturating_mul(2);
        let (source, destination) = if source_is_order {
            (&order[..], &mut scratch[..])
        } else {
            (&scratch[..], &mut order[..])
        };
        source
            .par_chunks(pair_width)
            .zip(destination.par_chunks_mut(pair_width))
            .try_for_each(|(source, destination)| -> QuickRowsResult<()> {
                check_cancellation()?;
                let right_start = width.min(source.len());
                let mut left = 0usize;
                let mut right = right_start;
                let mut output = 0usize;
                while left < right_start && right < source.len() {
                    if output.is_multiple_of(SORT_CANCELLATION_INTERVAL) {
                        check_cancellation()?;
                    }
                    if compare_rows(&source[left], &source[right]).is_le() {
                        destination[output] = source[left];
                        left += 1;
                    } else {
                        destination[output] = source[right];
                        right += 1;
                    }
                    output += 1;
                }
                destination[output..output + right_start - left]
                    .copy_from_slice(&source[left..right_start]);
                output += right_start - left;
                destination[output..output + source.len() - right]
                    .copy_from_slice(&source[right..]);
                check_cancellation()
            })?;
        source_is_order = !source_is_order;
        width = pair_width;
    }
    check_cancellation()?;
    Ok(if source_is_order { order } else { scratch })
}

pub struct SearchIndexBuild {
    path: PathBuf,
    settings: ParseSettings,
    offsets: Vec<u64>,
    mmap: Option<Arc<Mmap>>,
    _prepared_source: Option<Arc<tempfile::NamedTempFile>>,
    column_count: usize,
    edits: DocumentEdits,
    generation: u64,
    revision: u64,
}

#[derive(Debug)]
pub struct BuiltSearchIndex {
    columns: Vec<Option<ColumnSearchIndex>>,
    generation: u64,
    revision: u64,
}

impl SearchIndexBuild {
    pub fn build(
        self,
        cancellation: &CancellationToken,
        progress: Option<&dyn Fn(usize, usize)>,
    ) -> QuickRowsResult<BuiltSearchIndex> {
        cancellation.check()?;
        let mut columns = (0..self.column_count)
            .map(|_| Some(HashMap::new()))
            .collect::<Vec<Option<ColumnSearchIndex>>>();
        let row_count = self.offsets.len();
        let path = &self.path;
        for start in (0..row_count).step_by(INDEX_CHUNK_SIZE) {
            cancellation.check()?;
            let end = (start + INDEX_CHUNK_SIZE).min(row_count);
            let indices = (start..end).collect::<Vec<_>>();
            let mut warnings = Vec::new();
            let rows = match self.mmap.as_deref() {
                Some(mmap) => read_rows_by_index_mmap(
                    &mmap[..],
                    &self.offsets,
                    &indices,
                    &self.settings,
                    Some(self.column_count),
                    &mut warnings,
                ),
                None => read_rows_by_index(
                    path,
                    &self.offsets,
                    &indices,
                    &self.settings,
                    Some(self.column_count),
                    &mut warnings,
                ),
            }?;
            for (source_row, mut row) in (start..end).zip(rows) {
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                for (column, value) in row.into_iter().enumerate().take(self.column_count) {
                    let Some(index) = columns[column].as_mut() else {
                        continue;
                    };
                    index_value(index, value.to_lowercase(), source_row);
                    if index.len() > INDEX_MAX_CARDINALITY {
                        columns[column] = None;
                    }
                }
            }
            if let Some(progress) = progress {
                progress(end, row_count);
            }
            cancellation.check()?;
        }
        cancellation.check()?;
        for index in columns.iter_mut().flatten() {
            compact_index(index);
        }
        cancellation.check()?;
        Ok(BuiltSearchIndex {
            columns,
            generation: self.generation,
            revision: self.revision,
        })
    }
}
