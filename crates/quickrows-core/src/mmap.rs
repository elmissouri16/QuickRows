use memmap2::Mmap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[cfg(not(test))]
const MMAP_MIN_BYTES: u64 = 256 * 1024 * 1024;
#[cfg(test)]
const MMAP_MIN_BYTES: u64 = 1024;

/// Maps only files owned by QuickRows that will remain immutable for the
/// lifetime of the mapping. Live source files are read through normal file I/O
/// so an external truncate cannot invalidate pages and terminate the process.
pub(crate) fn open_immutable_mmap_if_large(
    path: &Path,
) -> Result<Option<Arc<Mmap>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() < MMAP_MIN_BYTES {
        return Ok(None);
    }

    let mmap = unsafe { Mmap::map(&file)? };
    Ok(Some(Arc::new(mmap)))
}

#[cfg(test)]
mod tests {
    use super::open_immutable_mmap_if_large;
    use std::io::Write;

    #[test]
    fn small_immutable_files_do_not_use_mmap() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(b"small").expect("write file");
        file.flush().expect("flush file");

        let result = open_immutable_mmap_if_large(file.path()).expect("open mmap");
        assert!(result.is_none());
    }
}
