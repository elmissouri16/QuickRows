use super::*;

pub(super) fn read_u64_payload<T>(
    reader: &mut impl Read,
    count: usize,
    mut convert: impl FnMut(u64) -> Option<T>,
) -> QuickRowsResult<Option<Vec<T>>> {
    let mut values = Vec::new();
    if values.try_reserve_exact(count).is_err() {
        return Ok(None);
    }
    let items_per_batch = CACHE_IO_BUFFER_BYTES / std::mem::size_of::<u64>();
    let mut bytes = vec![0u8; count.min(items_per_batch) * std::mem::size_of::<u64>()];
    let mut hasher = blake3::Hasher::new();
    let mut remaining = count;
    while remaining > 0 {
        let item_count = remaining.min(items_per_batch);
        let byte_count = item_count * std::mem::size_of::<u64>();
        let batch = &mut bytes[..byte_count];
        reader.read_exact(batch).map_err(QuickRowsError::from)?;
        hasher.update(batch);
        for encoded in batch.chunks_exact(std::mem::size_of::<u64>()) {
            let value = u64::from_le_bytes(encoded.try_into().expect("u64-sized cache chunk"));
            let Some(value) = convert(value) else {
                return Ok(None);
            };
            values.push(value);
        }
        remaining -= item_count;
    }
    let expected_checksum = read_hash(reader)?;
    if expected_checksum != *hasher.finalize().as_bytes() {
        return Ok(None);
    }
    Ok(Some(values))
}

pub(super) fn write_u64_payload(
    writer: &mut impl Write,
    values: impl IntoIterator<Item = u64>,
) -> QuickRowsResult<()> {
    let mut bytes = Vec::with_capacity(CACHE_IO_BUFFER_BYTES);
    let mut hasher = blake3::Hasher::new();
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
        if bytes.len() >= CACHE_IO_BUFFER_BYTES {
            hasher.update(&bytes);
            writer.write_all(&bytes).map_err(QuickRowsError::from)?;
            bytes.clear();
        }
    }
    if !bytes.is_empty() {
        hasher.update(&bytes);
        writer.write_all(&bytes).map_err(QuickRowsError::from)?;
    }
    write_hash(writer, hasher.finalize().as_bytes())
}

pub(super) fn read_hash(reader: &mut impl Read) -> QuickRowsResult<[u8; 32]> {
    let mut hash = [0u8; 32];
    reader.read_exact(&mut hash).map_err(QuickRowsError::from)?;
    Ok(hash)
}

pub(super) fn write_hash(writer: &mut impl Write, hash: &[u8; 32]) -> QuickRowsResult<()> {
    writer.write_all(hash).map_err(QuickRowsError::from)
}
