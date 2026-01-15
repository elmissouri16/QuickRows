use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::Manager;

const CACHE_VERSION: u32 = 1;
const CACHE_TTL: Duration = Duration::from_secs(60 * 60 * 24 * 3);

const OFFSETS_MAGIC: &[u8; 4] = b"CVOF";
const ORDER_MAGIC: &[u8; 4] = b"CVSO";

#[derive(Clone, Copy)]
pub struct CacheKey {
    pub hash: u64,
    pub len: u64,
    pub modified: u64,
}

pub fn cache_key(path: &str, settings_hash: Option<u64>) -> Result<CacheKey, String> {
    let meta = fs::metadata(path).map_err(|err| err.to_string())?;
    let len = meta.len();
    let modified = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);

    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    len.hash(&mut hasher);
    modified.hash(&mut hasher);
    if let Some(settings_hash) = settings_hash {
        settings_hash.hash(&mut hasher);
    }
    let hash = hasher.finish();

    Ok(CacheKey {
        hash,
        len,
        modified,
    })
}

pub fn ensure_cache_dir(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let base = app.path().app_data_dir().map_err(|err| err.to_string())?;
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
    if len != key.len || modified != key.modified {
        return Ok(None);
    }

    let count = read_u64(&mut file)? as usize;
    let mut offsets = vec![0u64; count];
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
    if len != key.len || modified != key.modified {
        return Ok(None);
    }

    let stored_column = read_u32(&mut file)? as usize;
    let stored_direction = read_u8(&mut file)?;
    let stored_ascending = stored_direction == 1;
    if stored_column != column || stored_ascending != ascending {
        return Ok(None);
    }

    let count = read_u64(&mut file)? as usize;
    let mut order = Vec::with_capacity(count);
    for _ in 0..count {
        let value = read_u64(&mut file)? as usize;
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
    write_u32(&mut file, column as u32)?;
    write_u8(&mut file, if ascending { 1 } else { 0 })?;
    write_u64(&mut file, order.len() as u64)?;
    for value in order {
        write_u64(&mut file, *value as u64)?;
    }
    Ok(())
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
        write_offsets_cache, write_order_cache,
    };

    #[test]
    fn offsets_cache_round_trip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");

        let key = cache_key(file_path.to_str().unwrap(), None).expect("cache key");
        let offsets_path = offsets_cache_path(dir.path(), key);
        let offsets = vec![0u64, 12, 16];

        write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
        let loaded = read_offsets_cache(&offsets_path, key)
            .expect("read offsets")
            .expect("offsets");
        assert_eq!(loaded, offsets);
    }

    #[test]
    fn order_cache_round_trip_and_mismatch() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("data.csv");
        std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");

        let key = cache_key(file_path.to_str().unwrap(), None).expect("cache key");
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
