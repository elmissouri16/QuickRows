//! Shared same-directory atomic persistence for disposable caches and durable settings.

use crate::error::{QuickRowsError, QuickRowsResult};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Durability {
    Flush,
    SyncFileAndDirectory,
}

pub(crate) fn write_file_atomically(
    path: &Path,
    temporary_prefix: &str,
    temporary_suffix: &str,
    buffer_capacity: usize,
    durability: Durability,
    write_contents: impl FnOnce(&mut BufWriter<&mut File>) -> QuickRowsResult<()>,
) -> QuickRowsResult<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(QuickRowsError::from)?;
    let mut temporary = tempfile::Builder::new()
        .prefix(temporary_prefix)
        .suffix(temporary_suffix)
        .tempfile_in(parent)
        .map_err(QuickRowsError::from)?;
    {
        let mut writer = BufWriter::with_capacity(buffer_capacity, temporary.as_file_mut());
        write_contents(&mut writer)?;
        writer.flush().map_err(QuickRowsError::from)?;
    }
    if durability == Durability::SyncFileAndDirectory {
        temporary
            .as_file()
            .sync_all()
            .map_err(QuickRowsError::from)?;
    }
    temporary
        .persist(path)
        .map_err(|error| QuickRowsError::from(error.error))?;
    if durability == Durability::SyncFileAndDirectory {
        sync_directory(parent)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> QuickRowsResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(QuickRowsError::from)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> QuickRowsResult<()> {
    Ok(())
}
