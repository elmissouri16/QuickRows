use crate::csv::ParseWarning;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CACHE_VERSION: u32 = 3;
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 3);
const MAX_CACHE_ALLOCATION_BYTES: u64 = 128 * 1024 * 1024;

const OFFSETS_MAGIC: &[u8; 4] = b"CVOF";
const ORDER_MAGIC: &[u8; 4] = b"CVSO";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileFingerprint {
    pub len: u64,
    pub modified: u64,
    pub content_hash: [u8; 32],
}

#[derive(Clone, Copy)]
pub struct CacheKey {
    pub hash: u64,
    pub len: u64,
    pub modified: u64,
    pub content_hash: [u8; 32],
}

pub fn file_fingerprint(path: impl AsRef<Path>) -> Result<FileFingerprint, String> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(|err| err.to_string())?;
    let metadata = file.metadata().map_err(|err| err.to_string())?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|err| err.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let content_hash = *hasher.finalize().as_bytes();
    Ok(FileFingerprint {
        len: metadata.len(),
        modified,
        content_hash,
    })
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

pub fn cache_key(path: impl AsRef<Path>, settings_hash: Option<u64>) -> Result<CacheKey, String> {
    let path = path.as_ref();
    let fingerprint = file_fingerprint(path)?;
    Ok(cache_key_from_fingerprint(path, settings_hash, fingerprint))
}

pub fn ensure_cache_dir(base: &Path) -> Result<PathBuf, String> {
    let dir = base.join("csv-index-cache");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
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
        if let Ok(age) = now.duration_since(modified) {
            if age > CACHE_TTL {
                let _ = fs::remove_file(&path);
            }
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

#[derive(Deserialize, Serialize)]
struct WarningCache {
    version: u32,
    len: u64,
    modified: u64,
    content_hash: [u8; 32],
    warnings: Vec<ParseWarning>,
}

pub fn read_warnings_cache(
    path: &Path,
    key: CacheKey,
) -> Result<Option<Vec<ParseWarning>>, String> {
    const MAX_WARNING_CACHE_BYTES: u64 = 4 * 1024 * 1024;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if metadata.len() > MAX_WARNING_CACHE_BYTES {
        return Ok(None);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_WARNING_CACHE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_WARNING_CACHE_BYTES {
        return Ok(None);
    }
    let cache: WarningCache = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if cache.version != CACHE_VERSION
        || cache.len != key.len
        || cache.modified != key.modified
        || cache.content_hash != key.content_hash
    {
        return Ok(None);
    }
    Ok(Some(cache.warnings))
}

pub fn write_warnings_cache(
    path: &Path,
    key: CacheKey,
    warnings: &[ParseWarning],
) -> Result<(), String> {
    let cache = WarningCache {
        version: CACHE_VERSION,
        len: key.len,
        modified: key.modified,
        content_hash: key.content_hash,
        warnings: warnings.to_vec(),
    };
    let bytes = serde_json::to_vec(&cache).map_err(|error| error.to_string())?;
    fs::write(path, bytes).map_err(|error| error.to_string())
}

pub fn read_offsets_cache(path: &Path, key: CacheKey) -> Result<Option<Vec<u64>>, String> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(None),
    };

    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() || magic != *OFFSETS_MAGIC {
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

    let count = match usize::try_from(read_u64(&mut file)?) {
        Ok(count) => count,
        Err(_) => return Ok(None),
    };
    let file_len = file.metadata().map_err(|err| err.to_string())?.len();
    let available_items = file_len.saturating_sub(64) / 8;
    if count as u64 > available_items
        || count as u64 > key.len.saturating_add(1)
        || (count as u64).saturating_mul(8) > MAX_CACHE_ALLOCATION_BYTES
    {
        return Ok(None);
    }
    let mut offsets = Vec::new();
    if offsets.try_reserve_exact(count).is_err() {
        return Ok(None);
    }
    offsets.resize(count, 0);
    for item in offsets.iter_mut() {
        *item = read_u64(&mut file)?;
    }

    Ok(Some(offsets))
}

pub fn write_offsets_cache(path: &Path, key: CacheKey, offsets: &[u64]) -> Result<(), String> {
    let mut file = File::create(path).map_err(|err| err.to_string())?;
    file.write_all(OFFSETS_MAGIC)
        .map_err(|err| err.to_string())?;
    write_u32(&mut file, CACHE_VERSION)?;
    write_u64(&mut file, key.len)?;
    write_u64(&mut file, key.modified)?;
    write_hash(&mut file, &key.content_hash)?;
    write_u64(&mut file, offsets.len() as u64)?;
    for value in offsets {
        write_u64(&mut file, *value)?;
    }
    Ok(())
}

pub fn read_order_cache(
    path: &Path,
    key: CacheKey,
    column: usize,
    ascending: bool,
) -> Result<Option<Vec<usize>>, String> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(None),
    };

    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() || magic != *ORDER_MAGIC {
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
    let stored_direction = read_u8(&mut file)?;
    let stored_ascending = stored_direction == 1;
    if stored_column != column || stored_ascending != ascending {
        return Ok(None);
    }

    let count = match usize::try_from(read_u64(&mut file)?) {
        Ok(count) => count,
        Err(_) => return Ok(None),
    };
    let file_len = file.metadata().map_err(|err| err.to_string())?.len();
    let available_items = file_len.saturating_sub(69) / 8;
    if count as u64 > available_items
        || count as u64 > key.len.saturating_add(1)
        || (count as u64).saturating_mul(8) > MAX_CACHE_ALLOCATION_BYTES
    {
        return Ok(None);
    }
    let mut order = Vec::new();
    if order.try_reserve_exact(count).is_err() {
        return Ok(None);
    }
    for _ in 0..count {
        let value = match usize::try_from(read_u64(&mut file)?) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        order.push(value);
    }

    Ok(Some(order))
}

pub fn write_order_cache(
    path: &Path,
    key: CacheKey,
    column: usize,
    ascending: bool,
    order: &[usize],
) -> Result<(), String> {
    let mut file = File::create(path).map_err(|err| err.to_string())?;
    file.write_all(ORDER_MAGIC).map_err(|err| err.to_string())?;
    write_u32(&mut file, CACHE_VERSION)?;
    write_u64(&mut file, key.len)?;
    write_u64(&mut file, key.modified)?;
    write_hash(&mut file, &key.content_hash)?;
    write_u32(&mut file, column as u32)?;
    write_u8(&mut file, if ascending { 1 } else { 0 })?;
    write_u64(&mut file, order.len() as u64)?;
    for value in order {
        write_u64(&mut file, *value as u64)?;
    }
    Ok(())
}

fn read_hash(reader: &mut impl Read) -> Result<[u8; 32], String> {
    let mut hash = [0u8; 32];
    reader
        .read_exact(&mut hash)
        .map_err(|err| err.to_string())?;
    Ok(hash)
}

fn write_hash(writer: &mut impl Write, hash: &[u8; 32]) -> Result<(), String> {
    writer.write_all(hash).map_err(|err| err.to_string())
}

fn read_u8(reader: &mut impl Read) -> Result<u8, String> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).map_err(|err| err.to_string())?;
    Ok(buf[0])
}

fn write_u8(writer: &mut impl Write, value: u8) -> Result<(), String> {
    writer.write_all(&[value]).map_err(|err| err.to_string())?;
    Ok(())
}

fn read_u32(reader: &mut impl Read) -> Result<u32, String> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(|err| err.to_string())?;
    Ok(u32::from_le_bytes(buf))
}

fn write_u32(writer: &mut impl Write, value: u32) -> Result<(), String> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|err| err.to_string())?;
    Ok(())
}

fn read_u64(reader: &mut impl Read) -> Result<u64, String> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).map_err(|err| err.to_string())?;
    Ok(u64::from_le_bytes(buf))
}

fn write_u64(writer: &mut impl Write, value: u64) -> Result<(), String> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        cache_key, offsets_cache_path, order_cache_path, read_offsets_cache, read_order_cache,
        read_warnings_cache, warnings_cache_path, write_offsets_cache, write_order_cache,
        MAX_CACHE_ALLOCATION_BYTES,
    };
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn offsets_cache_round_trip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");

        let key = cache_key(&file_path, None).expect("cache key");
        let offsets_path = offsets_cache_path(dir.path(), key);
        let offsets = vec![0u64, 12, 16];

        write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
        let loaded = read_offsets_cache(&offsets_path, key)
            .expect("read offsets")
            .expect("offsets");
        assert_eq!(loaded, offsets);
    }

    #[test]
    fn content_changes_invalidate_cache_even_with_matching_metadata() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"a,b\n1,2\n").expect("write first csv");
        let first = cache_key(&file_path, None).expect("first key");
        let offsets_path = offsets_cache_path(dir.path(), first);
        write_offsets_cache(&offsets_path, first, &[4]).expect("write offsets");

        std::fs::write(&file_path, b"a,b\n9,8\n").expect("write replacement csv");
        let mut replacement = cache_key(&file_path, None).expect("second key");
        assert_ne!(first.content_hash, replacement.content_hash);

        // Simulate a filesystem with a coarse or explicitly preserved mtime
        // and a colliding cache path. The cache header still rejects the bytes.
        replacement.hash = first.hash;
        replacement.modified = first.modified;
        assert!(read_offsets_cache(&offsets_path, replacement)
            .expect("read offsets")
            .is_none());
    }

    #[test]
    fn corrupt_counts_are_rejected_before_allocation() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"a,b\n1,2\n").unwrap();
        let key = cache_key(&file_path, None).unwrap();
        let offsets_path = offsets_cache_path(dir.path(), key);
        write_offsets_cache(&offsets_path, key, &[4]).unwrap();
        let mut bytes = std::fs::read(&offsets_path).unwrap();
        bytes[56..64].copy_from_slice(&u64::MAX.to_le_bytes());
        std::fs::write(&offsets_path, bytes).unwrap();
        assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
    }

    #[test]
    fn oversized_warning_cache_is_rejected_before_reading() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"a\n1\n").unwrap();
        let key = cache_key(&file_path, None).unwrap();
        let warnings_path = warnings_cache_path(dir.path(), key);
        let file = std::fs::File::create(&warnings_path).unwrap();
        file.set_len(4 * 1024 * 1024 + 1).unwrap();

        assert!(read_warnings_cache(&warnings_path, key).unwrap().is_none());
    }

    #[test]
    fn oversized_offset_allocation_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"a\n1\n").unwrap();
        let mut key = cache_key(&file_path, None).unwrap();
        key.len = u64::MAX;
        let offsets_path = offsets_cache_path(dir.path(), key);
        write_offsets_cache(&offsets_path, key, &[]).unwrap();
        let count = MAX_CACHE_ALLOCATION_BYTES / 8 + 1;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&offsets_path)
            .unwrap();
        file.seek(SeekFrom::Start(56)).unwrap();
        file.write_all(&count.to_le_bytes()).unwrap();
        file.set_len(64 + count * 8).unwrap();

        assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
    }

    #[test]
    fn order_cache_round_trip_and_mismatch() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");

        let key = cache_key(&file_path, None).expect("cache key");
        let order_path = order_cache_path(dir.path(), key, 2, true);
        let order = vec![2usize, 0, 1];

        write_order_cache(&order_path, key, 2, true, &order).expect("write order");
        let loaded = read_order_cache(&order_path, key, 2, true)
            .expect("read order")
            .expect("order");
        assert_eq!(loaded, order);

        let wrong_column = read_order_cache(&order_path, key, 1, true).expect("read order");
        assert!(wrong_column.is_none());

        let wrong_direction = read_order_cache(&order_path, key, 2, false).expect("read order");
        assert!(wrong_direction.is_none());
    }
}
