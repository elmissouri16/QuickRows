use super::*;
use std::fs::File;
use std::path::Path;
use std::time::UNIX_EPOCH;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileFingerprint {
    pub len: u64,
    pub modified: u64,
    pub content_hash: [u8; 32],
}

pub fn file_fingerprint(path: impl AsRef<Path>) -> QuickRowsResult<FileFingerprint> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(QuickRowsError::from)?;
    let metadata = file.metadata().map_err(QuickRowsError::from)?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(QuickRowsError::from)?;
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
