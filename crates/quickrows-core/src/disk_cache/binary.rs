use super::checksum::{read_hash, read_u64_payload, write_hash, write_u64_payload};
use super::store::{CacheKey, validate_cache_payload, write_cache_atomically};
use super::*;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

pub fn read_offsets_cache(path: &Path, key: CacheKey) -> QuickRowsResult<Option<Vec<u64>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(None),
    };
    let file_len = file.metadata().map_err(QuickRowsError::from)?.len();
    let mut file = BufReader::with_capacity(CACHE_IO_BUFFER_BYTES, file);

    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() || magic != *OFFSETS_MAGIC {
        return Ok(None);
    }
    if file_len < OFFSETS_HEADER_BYTES.saturating_add(PAYLOAD_CHECKSUM_BYTES) {
        return Ok(None);
    }

    let version = read_u32(&mut file)?;
    if version != CACHE_VERSION {
        return Ok(None);
    }

    let len = read_u64(&mut file)?;
    let modified = read_u64(&mut file)?;
    let content_hash = read_hash(&mut file)?;
    if len != key.len || modified != key.modified || content_hash != key.content_hash {
        return Ok(None);
    }

    let stored_count = read_u64(&mut file)?;
    let count = match usize::try_from(stored_count) {
        Ok(count) => count,
        Err(_) => return Ok(None),
    };
    let Some(payload_bytes) = stored_count.checked_mul(std::mem::size_of::<u64>() as u64) else {
        return Ok(None);
    };
    let expected_file_len = OFFSETS_HEADER_BYTES
        .checked_add(payload_bytes)
        .and_then(|length| length.checked_add(PAYLOAD_CHECKSUM_BYTES));
    if stored_count > key.len.saturating_add(1)
        || payload_bytes > MAX_CACHE_ALLOCATION_BYTES
        || expected_file_len != Some(file_len)
    {
        return Ok(None);
    }

    read_u64_payload(&mut file, count, Some)
}

pub fn write_offsets_cache(path: &Path, key: CacheKey, offsets: &[u64]) -> QuickRowsResult<()> {
    let count = validate_cache_payload(path, key, offsets.len(), "Offset")?;
    write_cache_atomically(path, |writer| {
        writer
            .write_all(OFFSETS_MAGIC)
            .map_err(QuickRowsError::from)?;
        write_u32(writer, CACHE_VERSION)?;
        write_u64(writer, key.len)?;
        write_u64(writer, key.modified)?;
        write_hash(writer, &key.content_hash)?;
        write_u64(writer, count)?;
        write_u64_payload(writer, offsets.iter().copied())
    })
}

pub fn read_order_cache(
    path: &Path,
    key: CacheKey,
    column: usize,
    ascending: bool,
) -> QuickRowsResult<Option<Vec<usize>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(None),
    };
    let file_len = file.metadata().map_err(QuickRowsError::from)?.len();
    let mut file = BufReader::with_capacity(CACHE_IO_BUFFER_BYTES, file);

    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() || magic != *ORDER_MAGIC {
        return Ok(None);
    }
    if file_len < ORDER_HEADER_BYTES.saturating_add(PAYLOAD_CHECKSUM_BYTES) {
        return Ok(None);
    }

    let version = read_u32(&mut file)?;
    if version != CACHE_VERSION {
        return Ok(None);
    }

    let len = read_u64(&mut file)?;
    let modified = read_u64(&mut file)?;
    let content_hash = read_hash(&mut file)?;
    if len != key.len || modified != key.modified || content_hash != key.content_hash {
        return Ok(None);
    }

    let stored_column = read_u32(&mut file)? as usize;
    let stored_ascending = match read_u8(&mut file)? {
        0 => false,
        1 => true,
        _ => return Ok(None),
    };
    if stored_column != column || stored_ascending != ascending {
        return Ok(None);
    }

    let stored_count = read_u64(&mut file)?;
    let count = match usize::try_from(stored_count) {
        Ok(count) => count,
        Err(_) => return Ok(None),
    };
    let Some(payload_bytes) = stored_count.checked_mul(std::mem::size_of::<u64>() as u64) else {
        return Ok(None);
    };
    let expected_file_len = ORDER_HEADER_BYTES
        .checked_add(payload_bytes)
        .and_then(|length| length.checked_add(PAYLOAD_CHECKSUM_BYTES));
    if stored_count > key.len.saturating_add(1)
        || payload_bytes > MAX_CACHE_ALLOCATION_BYTES
        || expected_file_len != Some(file_len)
    {
        return Ok(None);
    }

    read_u64_payload(&mut file, count, |value| usize::try_from(value).ok())
}

pub fn write_order_cache(
    path: &Path,
    key: CacheKey,
    column: usize,
    ascending: bool,
    order: &[usize],
) -> QuickRowsResult<()> {
    let stored_column = match u32::try_from(column) {
        Ok(column) => column,
        Err(error) => {
            let _ = fs::remove_file(path);
            return Err(QuickRowsError::cache_corrupt(error.to_string()));
        }
    };
    let count = validate_cache_payload(path, key, order.len(), "Sort")?;
    write_cache_atomically(path, |writer| {
        writer
            .write_all(ORDER_MAGIC)
            .map_err(QuickRowsError::from)?;
        write_u32(writer, CACHE_VERSION)?;
        write_u64(writer, key.len)?;
        write_u64(writer, key.modified)?;
        write_hash(writer, &key.content_hash)?;
        write_u32(writer, stored_column)?;
        write_u8(writer, if ascending { 1 } else { 0 })?;
        write_u64(writer, count)?;
        write_u64_payload(writer, order.iter().map(|&value| value as u64))
    })
}

fn read_u8(reader: &mut impl Read) -> QuickRowsResult<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).map_err(QuickRowsError::from)?;
    Ok(buf[0])
}

fn write_u8(writer: &mut impl Write, value: u8) -> QuickRowsResult<()> {
    writer.write_all(&[value]).map_err(QuickRowsError::from)?;
    Ok(())
}

fn read_u32(reader: &mut impl Read) -> QuickRowsResult<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(QuickRowsError::from)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_u32(writer: &mut impl Write, value: u32) -> QuickRowsResult<()> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(QuickRowsError::from)?;
    Ok(())
}

fn read_u64(reader: &mut impl Read) -> QuickRowsResult<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).map_err(QuickRowsError::from)?;
    Ok(u64::from_le_bytes(buf))
}

fn write_u64(writer: &mut impl Write, value: u64) -> QuickRowsResult<()> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(QuickRowsError::from)?;
    Ok(())
}
