use super::store::{CacheKey, write_cache_atomically};
use super::*;
use crate::csv::ParseWarning;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

#[derive(Deserialize, Serialize)]
struct WarningCache {
    version: u32,
    len: u64,
    modified: u64,
    content_hash: [u8; 32],
    warnings_checksum: [u8; 32],
    warnings: Vec<ParseWarning>,
}

fn warnings_checksum(warnings: &[ParseWarning]) -> QuickRowsResult<[u8; 32]> {
    let bytes = serde_json::to_vec(warnings)
        .map_err(|error| QuickRowsError::cache_corrupt(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

pub fn read_warnings_cache(
    path: &Path,
    key: CacheKey,
) -> QuickRowsResult<Option<Vec<ParseWarning>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(QuickRowsError::from(error)),
    };
    let metadata = file.metadata().map_err(QuickRowsError::from)?;
    if metadata.len() > MAX_WARNING_CACHE_BYTES {
        return Ok(None);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_WARNING_CACHE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(QuickRowsError::from)?;
    if bytes.len() as u64 > MAX_WARNING_CACHE_BYTES {
        return Ok(None);
    }
    let cache: WarningCache = serde_json::from_slice(&bytes)
        .map_err(|error| QuickRowsError::cache_corrupt(error.to_string()))?;
    if cache.version != CACHE_VERSION
        || cache.len != key.len
        || cache.modified != key.modified
        || cache.content_hash != key.content_hash
        || warnings_checksum(&cache.warnings)? != cache.warnings_checksum
    {
        return Ok(None);
    }
    Ok(Some(cache.warnings))
}

pub fn write_warnings_cache(
    path: &Path,
    key: CacheKey,
    warnings: &[ParseWarning],
) -> QuickRowsResult<()> {
    let cache = WarningCache {
        version: CACHE_VERSION,
        len: key.len,
        modified: key.modified,
        content_hash: key.content_hash,
        warnings_checksum: warnings_checksum(warnings)?,
        warnings: warnings.to_vec(),
    };
    let bytes = serde_json::to_vec(&cache)
        .map_err(|error| QuickRowsError::cache_corrupt(error.to_string()))?;
    if bytes.len() as u64 > MAX_WARNING_CACHE_BYTES {
        let _ = fs::remove_file(path);
        return Err(QuickRowsError::cache_corrupt(
            "Warning cache exceeds the maximum cache size",
        ));
    }
    write_cache_atomically(path, |writer| {
        writer.write_all(&bytes).map_err(QuickRowsError::from)
    })
}
