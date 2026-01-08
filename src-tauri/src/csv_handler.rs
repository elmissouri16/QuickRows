use csv::{ByteRecord, Position, ReaderBuilder, StringRecord};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};

pub fn get_headers(path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    let headers = rdr
        .headers()?
        .iter()
        .map(|header| header.to_string())
        .collect();
    Ok(headers)
}

fn build_row_offsets_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let _ = rdr.headers()?;
    let mut offsets = Vec::new();
    let mut record = ByteRecord::new();
    loop {
        let pos = rdr.position().byte();
        if !rdr.read_byte_record(&mut record)? {
            break;
        }
        offsets.push(pos);
    }
    Ok(offsets)
}

fn read_chunk_from_reader<R: Read>(
    mut rdr: csv::Reader<R>,
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let rows = rdr
        .records()
        .skip(start)
        .take(count)
        .filter_map(|record| record.ok())
        .map(|record| record.iter().map(|field| field.to_string()).collect())
        .collect();
    Ok(rows)
}

fn read_chunk_with_offsets_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    if start >= offsets.len() {
        return Ok(Vec::new());
    }

    let end = usize::min(start + count, offsets.len());
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;
    let mut record = StringRecord::new();
    let mut rows = Vec::with_capacity(end - start);

    for _ in start..end {
        if !rdr.read_record(&mut record)? {
            break;
        }
        rows.push(record.iter().map(|field| field.to_string()).collect());
    }

    Ok(rows)
}

fn read_rows_by_index_from_reader<R: Read + Seek>(
    mut rdr: csv::Reader<R>,
    offsets: &[u64],
    indices: &[usize],
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    if indices.is_empty() {
        return Ok(Vec::new());
    }

    let mut record = StringRecord::new();
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

        if !rdr.read_record(&mut record)? {
            continue;
        }

        rows[order_idx] = record.iter().map(|field| field.to_string()).collect();
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
    query_lower: &str,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    if start >= offsets.len() {
        return Ok(Vec::new());
    }

    let end = usize::min(end, offsets.len());
    let mut position = Position::new();
    position.set_byte(offsets[start]);
    rdr.seek(position)?;

    let mut record = StringRecord::new();
    let mut matches = Vec::new();
    for row_index in start..end {
        if !rdr.read_record(&mut record)? {
            break;
        }
        let is_match = match column_idx {
            Some(index) => record
                .get(index)
                .unwrap_or("")
                .to_lowercase()
                .contains(query_lower),
            None => record
                .iter()
                .any(|cell| cell.to_lowercase().contains(query_lower)),
        };
        if is_match {
            matches.push(row_index);
        }
    }

    Ok(matches)
}

pub fn build_row_offsets(path: &str) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = ReaderBuilder::new().has_headers(true).from_reader(reader);
    build_row_offsets_from_reader(rdr)
}

pub fn build_row_offsets_mmap(data: &[u8]) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let rdr = ReaderBuilder::new().has_headers(true).from_reader(data);
    build_row_offsets_from_reader(rdr)
}

pub fn read_chunk(
    path: &str,
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = ReaderBuilder::new().has_headers(true).from_reader(reader);
    read_chunk_from_reader(rdr, start, count)
}

pub fn read_chunk_mmap(
    data: &[u8],
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let rdr = ReaderBuilder::new().has_headers(true).from_reader(data);
    read_chunk_from_reader(rdr, start, count)
}

pub fn read_chunk_with_offsets(
    path: &str,
    offsets: &[u64],
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(reader);
    read_chunk_with_offsets_from_reader(rdr, offsets, start, count)
}

pub fn read_chunk_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    count: usize,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(data);
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(cursor);
    read_chunk_with_offsets_from_reader(rdr, offsets, start, count)
}

pub fn read_rows_by_index(
    path: &str,
    offsets: &[u64],
    indices: &[usize],
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(reader);
    read_rows_by_index_from_reader(rdr, offsets, indices)
}

pub fn read_rows_by_index_mmap(
    data: &[u8],
    offsets: &[u64],
    indices: &[usize],
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(data);
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(cursor);
    read_rows_by_index_from_reader(rdr, offsets, indices)
}

pub fn search_range_with_offsets(
    path: &str,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query_lower: &str,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(reader);
    search_range_with_offsets_from_reader(rdr, offsets, start, end, column_idx, query_lower)
}

pub fn search_range_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query_lower: &str,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let cursor = Cursor::new(data);
    let rdr = ReaderBuilder::new().has_headers(false).from_reader(cursor);
    search_range_with_offsets_from_reader(rdr, offsets, start, end, column_idx, query_lower)
}
