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
