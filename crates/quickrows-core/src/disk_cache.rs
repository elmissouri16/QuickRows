use crate::csv::ParseWarning;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CACHE_VERSION: u32 = 4;
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const CACHE_IO_BUFFER_BYTES: usize = 1024 * 1024;
const CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 3);
const MAX_CACHE_ALLOCATION_BYTES: u64 = 128 * 1024 * 1024;
const MAX_WARNING_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const OFFSETS_HEADER_BYTES: u64 = 64;
const ORDER_HEADER_BYTES: u64 = 69;
const PAYLOAD_CHECKSUM_BYTES: u64 = 32;

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

// Cache files are disposable, so a flushed same-directory rename is sufficient:
// readers see either the previous complete file or the new complete file.
fn write_cache_atomically(
    path: &Path,
    write_contents: impl FnOnce(&mut BufWriter<&mut File>) -> Result<(), String>,
) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".quickrows-cache-")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|error| error.to_string())?;
    {
        let mut writer = BufWriter::with_capacity(CACHE_IO_BUFFER_BYTES, temporary.as_file_mut());
        write_contents(&mut writer)?;
        writer.flush().map_err(|error| error.to_string())?;
    }
    temporary
        .persist(path)
        .map_err(|error| error.error.to_string())?;
    Ok(())
}

fn validate_cache_payload(
    path: &Path,
    key: CacheKey,
    count: usize,
    cache_name: &str,
) -> Result<u64, String> {
    let stored_count = u64::try_from(count).map_err(|error| error.to_string())?;
    let payload_bytes = stored_count
        .checked_mul(std::mem::size_of::<u64>() as u64)
        .ok_or_else(|| format!("{cache_name} cache size overflowed"))?;
    if stored_count > key.len.saturating_add(1) || payload_bytes > MAX_CACHE_ALLOCATION_BYTES {
        let _ = fs::remove_file(path);
        return Err(format!("{cache_name} cache exceeds the maximum cache size"));
    }
    Ok(stored_count)
}

#[derive(Deserialize, Serialize)]
struct WarningCache {
    version: u32,
    len: u64,
    modified: u64,
    content_hash: [u8; 32],
    warnings_checksum: [u8; 32],
    warnings: Vec<ParseWarning>,
}

fn warnings_checksum(warnings: &[ParseWarning]) -> Result<[u8; 32], String> {
    let bytes = serde_json::to_vec(warnings).map_err(|error| error.to_string())?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

pub fn read_warnings_cache(
    path: &Path,
    key: CacheKey,
) -> Result<Option<Vec<ParseWarning>>, String> {
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
) -> Result<(), String> {
    let cache = WarningCache {
        version: CACHE_VERSION,
        len: key.len,
        modified: key.modified,
        content_hash: key.content_hash,
        warnings_checksum: warnings_checksum(warnings)?,
        warnings: warnings.to_vec(),
    };
    let bytes = serde_json::to_vec(&cache).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_WARNING_CACHE_BYTES {
        let _ = fs::remove_file(path);
        return Err("Warning cache exceeds the maximum cache size".to_string());
    }
    write_cache_atomically(path, |writer| {
        writer.write_all(&bytes).map_err(|error| error.to_string())
    })
}

fn read_u64_payload<T>(
    reader: &mut impl Read,
    count: usize,
    mut convert: impl FnMut(u64) -> Option<T>,
) -> Result<Option<Vec<T>>, String> {
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
        reader
            .read_exact(batch)
            .map_err(|error| error.to_string())?;
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

fn write_u64_payload(
    writer: &mut impl Write,
    values: impl IntoIterator<Item = u64>,
) -> Result<(), String> {
    let mut bytes = Vec::with_capacity(CACHE_IO_BUFFER_BYTES);
    let mut hasher = blake3::Hasher::new();
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
        if bytes.len() >= CACHE_IO_BUFFER_BYTES {
            hasher.update(&bytes);
            writer
                .write_all(&bytes)
                .map_err(|error| error.to_string())?;
            bytes.clear();
        }
    }
    if !bytes.is_empty() {
        hasher.update(&bytes);
        writer
            .write_all(&bytes)
            .map_err(|error| error.to_string())?;
    }
    write_hash(writer, hasher.finalize().as_bytes())
}

pub fn read_offsets_cache(path: &Path, key: CacheKey) -> Result<Option<Vec<u64>>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(None),
    };
    let file_len = file.metadata().map_err(|err| err.to_string())?.len();
    let mut file = BufReader::with_capacity(CACHE_IO_BUFFER_BYTES, file);

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

pub fn write_offsets_cache(path: &Path, key: CacheKey, offsets: &[u64]) -> Result<(), String> {
    let count = validate_cache_payload(path, key, offsets.len(), "Offset")?;
    write_cache_atomically(path, |writer| {
        writer
            .write_all(OFFSETS_MAGIC)
            .map_err(|err| err.to_string())?;
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
) -> Result<Option<Vec<usize>>, String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Ok(None),
    };
    let file_len = file.metadata().map_err(|err| err.to_string())?.len();
    let mut file = BufReader::with_capacity(CACHE_IO_BUFFER_BYTES, file);

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
) -> Result<(), String> {
    let stored_column = match u32::try_from(column) {
        Ok(column) => column,
        Err(error) => {
            let _ = fs::remove_file(path);
            return Err(error.to_string());
        }
    };
    let count = validate_cache_payload(path, key, order.len(), "Sort")?;
    write_cache_atomically(path, |writer| {
        writer
            .write_all(ORDER_MAGIC)
            .map_err(|err| err.to_string())?;
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
        read_warnings_cache, validate_cache_payload, warnings_cache_path, write_cache_atomically,
        write_offsets_cache, write_order_cache, write_warnings_cache, MAX_CACHE_ALLOCATION_BYTES,
    };
    use crate::csv::ParseWarning;
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
    fn offsets_cache_rejects_modified_truncated_and_extended_payloads() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");
        let key = cache_key(&file_path, None).expect("cache key");
        let offsets_path = offsets_cache_path(dir.path(), key);
        let offsets = vec![0u64, 12, 16];
        write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
        let valid = std::fs::read(&offsets_path).unwrap();

        let mut modified = valid.clone();
        modified[64] ^= 0xff;
        std::fs::write(&offsets_path, modified).unwrap();
        assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());

        std::fs::write(&offsets_path, &valid[..valid.len() - 1]).unwrap();
        assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());

        let mut extended = valid;
        extended.push(0);
        std::fs::write(&offsets_path, extended).unwrap();
        assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
    }

    #[test]
    fn zeroed_offset_count_cannot_turn_a_populated_csv_into_an_empty_cache() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");
        let key = cache_key(&file_path, None).expect("cache key");
        let offsets_path = offsets_cache_path(dir.path(), key);
        write_offsets_cache(&offsets_path, key, &[12, 16]).expect("write offsets");
        let mut bytes = std::fs::read(&offsets_path).unwrap();
        bytes[56..64].copy_from_slice(&0u64.to_le_bytes());
        std::fs::write(&offsets_path, bytes).unwrap();

        assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
    }

    #[test]
    fn buffered_cache_round_trip_crosses_multiple_io_buffers() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1\n1\n").expect("write csv");
        let mut key = cache_key(&file_path, None).expect("cache key");
        key.len = u64::MAX;
        let offsets_path = offsets_cache_path(dir.path(), key);
        let offsets = (0..200_000u64).collect::<Vec<_>>();

        write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
        let loaded = read_offsets_cache(&offsets_path, key)
            .expect("read offsets")
            .expect("offsets");
        assert_eq!(loaded, offsets);
    }

    #[test]
    fn failed_atomic_cache_write_preserves_existing_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("cache.bin");
        std::fs::write(&path, b"existing cache").expect("write existing cache");

        let error = write_cache_atomically(&path, |writer| {
            writer
                .write_all(b"partial replacement")
                .map_err(|error| error.to_string())?;
            Err("injected cache write failure".to_string())
        })
        .expect_err("cache write should fail");

        assert_eq!(error, "injected cache write failure");
        assert_eq!(std::fs::read(&path).unwrap(), b"existing cache");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn atomic_cache_write_replaces_existing_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("cache.bin");
        std::fs::write(&path, b"existing cache").expect("write existing cache");

        write_cache_atomically(&path, |writer| {
            writer
                .write_all(b"replacement cache")
                .map_err(|error| error.to_string())
        })
        .expect("replace cache");

        assert_eq!(std::fs::read(&path).unwrap(), b"replacement cache");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_atomic_persist_removes_the_temporary_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let occupied = dir.path().join("occupied");
        std::fs::create_dir(&occupied).expect("create occupied destination");

        assert!(write_cache_atomically(&occupied, |writer| {
            writer
                .write_all(b"replacement cache")
                .map_err(|error| error.to_string())
        })
        .is_err());

        assert!(occupied.is_dir());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn oversized_cache_write_removes_an_existing_unusable_entry() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"a\n").unwrap();
        let mut key = cache_key(&file_path, None).unwrap();
        key.len = u64::MAX;
        let path = offsets_cache_path(dir.path(), key);
        std::fs::write(&path, b"stale cache").unwrap();
        let oversized_count = (MAX_CACHE_ALLOCATION_BYTES / 8 + 1) as usize;

        assert!(validate_cache_payload(&path, key, oversized_count, "Offset").is_err());
        assert!(!path.exists());
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
    fn warnings_cache_rejects_valid_json_with_modified_diagnostics() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"a\n1\n").unwrap();
        let key = cache_key(&file_path, None).unwrap();
        let warnings_path = warnings_cache_path(dir.path(), key);
        let warnings = vec![ParseWarning {
            record: Some(123),
            line: Some(4),
            byte: Some(8),
            field: Some(1),
            kind: "test".to_string(),
            message: "test warning".to_string(),
            expected_len: Some(2),
            len: Some(3),
        }];
        write_warnings_cache(&warnings_path, key, &warnings).unwrap();
        assert_eq!(
            read_warnings_cache(&warnings_path, key).unwrap().unwrap()[0].record,
            Some(123)
        );

        let bytes = std::fs::read(&warnings_path).unwrap();
        let json = String::from_utf8(bytes).unwrap();
        let modified = json.replacen("\"record\":123", "\"record\":124", 1);
        assert_ne!(modified, json);
        std::fs::write(&warnings_path, modified).unwrap();

        assert!(read_warnings_cache(&warnings_path, key).unwrap().is_none());
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

        let mut invalid_direction = std::fs::read(&order_path).unwrap();
        invalid_direction[60] = 2;
        std::fs::write(&order_path, invalid_direction).unwrap();
        assert!(read_order_cache(&order_path, key, 2, false)
            .expect("read invalid direction")
            .is_none());

        write_order_cache(&order_path, key, 2, true, &order).unwrap();
        let mut corrupted = std::fs::read(&order_path).unwrap();
        corrupted[69] ^= 0xff;
        std::fs::write(&order_path, corrupted).unwrap();
        assert!(read_order_cache(&order_path, key, 2, true)
            .expect("read corrupted order")
            .is_none());
    }
}
