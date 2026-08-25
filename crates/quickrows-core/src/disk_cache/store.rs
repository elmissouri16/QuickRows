use super::fingerprint::{FileFingerprint, file_fingerprint};
use super::*;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 3);

#[derive(Clone, Copy)]
pub struct CacheKey {
    pub hash: u64,
    pub len: u64,
    pub modified: u64,
    pub content_hash: [u8; 32],
}

pub fn cache_key_from_fingerprint(
    path: impl AsRef<Path>,
    settings_hash: Option<u64>,
    fingerprint: FileFingerprint,
) -> CacheKey {
    let mut hasher = DefaultHasher::new();
    path.as_ref().hash(&mut hasher);
    fingerprint.len.hash(&mut hasher);
    fingerprint.modified.hash(&mut hasher);
    fingerprint.content_hash.hash(&mut hasher);
    if let Some(settings_hash) = settings_hash {
        settings_hash.hash(&mut hasher);
    }
    let hash = hasher.finish();

    CacheKey {
        hash,
        len: fingerprint.len,
        modified: fingerprint.modified,
        content_hash: fingerprint.content_hash,
    }
}

pub fn cache_key(path: impl AsRef<Path>, settings_hash: Option<u64>) -> QuickRowsResult<CacheKey> {
    let path = path.as_ref();
    let fingerprint = file_fingerprint(path)?;
    Ok(cache_key_from_fingerprint(path, settings_hash, fingerprint))
}

pub fn ensure_cache_dir(base: &Path) -> QuickRowsResult<PathBuf> {
    let dir = base.join("csv-index-cache");
    fs::create_dir_all(&dir).map_err(QuickRowsError::from)?;
    Ok(dir)
}

pub fn prune_cache_dir(dir: &Path) {
    let now = SystemTime::now();
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if let Ok(age) = now.duration_since(modified)
            && age > CACHE_TTL
        {
            let _ = fs::remove_file(&path);
        }
    }
}

pub fn offsets_cache_path(dir: &Path, key: CacheKey) -> PathBuf {
    dir.join(format!("offsets_{:016x}.bin", key.hash))
}

pub fn order_cache_path(dir: &Path, key: CacheKey, column: usize, ascending: bool) -> PathBuf {
    dir.join(format!(
        "order_{:016x}_c{}_{}.bin",
        key.hash,
        column,
        if ascending { "asc" } else { "desc" }
    ))
}

pub fn warnings_cache_path(dir: &Path, key: CacheKey) -> PathBuf {
    dir.join(format!("warnings_{:016x}.json", key.hash))
}

// Cache files are disposable, so a flushed same-directory rename is sufficient:
// readers see either the previous complete file or the new complete file.
pub(super) fn write_cache_atomically(
    path: &Path,
    write_contents: impl FnOnce(&mut std::io::BufWriter<&mut std::fs::File>) -> QuickRowsResult<()>,
) -> QuickRowsResult<()> {
    crate::storage::write_file_atomically(
        path,
        ".quickrows-cache-",
        ".tmp",
        CACHE_IO_BUFFER_BYTES,
        crate::storage::Durability::Flush,
        write_contents,
    )
}

pub(super) fn validate_cache_payload(
    path: &Path,
    key: CacheKey,
    count: usize,
    cache_name: &str,
) -> QuickRowsResult<u64> {
    let stored_count =
        u64::try_from(count).map_err(|error| QuickRowsError::cache_corrupt(error.to_string()))?;
    let payload_bytes = stored_count
        .checked_mul(std::mem::size_of::<u64>() as u64)
        .ok_or_else(|| {
            QuickRowsError::cache_corrupt(format!("{cache_name} cache size overflowed"))
        })?;
    if stored_count > key.len.saturating_add(1) || payload_bytes > MAX_CACHE_ALLOCATION_BYTES {
        let _ = fs::remove_file(path);
        return Err(QuickRowsError::cache_corrupt(format!(
            "{cache_name} cache exceeds the maximum cache size"
        )));
    }
    Ok(stored_count)
}
