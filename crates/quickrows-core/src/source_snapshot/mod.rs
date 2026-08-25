//! Stable source observation and canonical snapshot capture.

use crate::disk_cache::FileFingerprint;
use crate::error::{QuickRowsError, QuickRowsResult};
use std::io::{Read, Seek, Write};
use std::path::Path;

const SOURCE_SNAPSHOT_BUFFER_BYTES: usize = 1024 * 1024;

fn metadata_modified(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

fn check_snapshot_cancellation(is_cancelled: &dyn Fn() -> bool) -> QuickRowsResult<()> {
    if is_cancelled() {
        Err(QuickRowsError::cancelled())
    } else {
        Ok(())
    }
}

fn hash_snapshot(
    source: &mut std::fs::File,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<(u64, [u8; 32])> {
    let mut hasher = blake3::Hasher::new();
    let mut hashed = 0u64;
    let mut buffer = vec![0u8; SOURCE_SNAPSHOT_BUFFER_BYTES];
    loop {
        check_snapshot_cancellation(is_cancelled)?;
        let read = source.read(&mut buffer).map_err(QuickRowsError::from)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        hashed = hashed.saturating_add(read as u64);
    }
    Ok((hashed, *hasher.finalize().as_bytes()))
}

mod fingerprint;
mod open_state;
mod snapshot;

pub(crate) use fingerprint::file_fingerprint_cancellable;
pub(crate) use open_state::{
    OpenFileState, capture_open_file_state, verify_open_file_state,
    verify_path_references_open_file,
};
pub(crate) use snapshot::{SourceSnapshot, snapshot_csv_source};

#[cfg(test)]
mod tests;
