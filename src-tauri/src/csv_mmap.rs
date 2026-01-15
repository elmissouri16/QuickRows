use memmap2::Mmap;
use std::fs::File;
use std::sync::Arc;

const MMAP_MIN_BYTES: u64 = 256 * 1024 * 1024;

pub fn open_mmap_if_large(path: &str) -> Result<Option<Arc<Mmap>>, Box<dyn std::error::Error>> {
    let metadata = std::fs::metadata(path)?;
    if metadata.len() < MMAP_MIN_BYTES {
        return Ok(None);
    }

    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    Ok(Some(Arc::new(mmap)))
}

#[cfg(test)]
mod tests {
    use super::open_mmap_if_large;
    use std::io::Write;

    #[test]
    fn small_files_do_not_use_mmap() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(b"small").expect("write file");
        file.flush().expect("flush file");

        let result = open_mmap_if_large(file.path().to_str().unwrap()).expect("open mmap");
        assert!(result.is_none());
    }
}
