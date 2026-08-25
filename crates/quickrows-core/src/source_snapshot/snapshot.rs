use super::*;

pub(crate) struct SourceSnapshot {
    pub(crate) temporary: tempfile::NamedTempFile,
    pub(crate) fingerprint: FileFingerprint,
}

pub(super) fn copy_snapshot(
    source: &mut std::fs::File,
    destination: &mut std::fs::File,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<(u64, [u8; 32])> {
    let mut hasher = blake3::Hasher::new();
    let mut copied = 0u64;
    let mut buffer = vec![0u8; SOURCE_SNAPSHOT_BUFFER_BYTES];
    loop {
        check_snapshot_cancellation(is_cancelled)?;
        let read = source.read(&mut buffer).map_err(QuickRowsError::from)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        copied = copied.saturating_add(read as u64);
        destination
            .write_all(&buffer[..read])
            .map_err(QuickRowsError::from)?;
    }
    destination.flush().map_err(QuickRowsError::from)?;
    Ok((copied, *hasher.finalize().as_bytes()))
}

// A failed capture returns its created file through Retry so the caller can
// remove only the destination it owns before starting the stream fallback.
pub(super) enum SnapshotCapture {
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        test
    ))]
    Captured(std::fs::File),
    #[cfg(any(target_os = "linux", test))]
    Retry(std::fs::File),
    #[cfg(any(not(target_os = "linux"), test))]
    Unsupported,
}

#[cfg(target_os = "linux")]
pub(super) fn try_reflink_snapshot(
    source: &std::fs::File,
    destination: &Path,
    source_len: u64,
) -> std::io::Result<SnapshotCapture> {
    let snapshot = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination)?;
    let Some(source_len) = std::num::NonZeroU64::new(source_len) else {
        return Ok(SnapshotCapture::Captured(snapshot));
    };
    if reflink_copy::ReflinkBlockBuilder::new(source, &snapshot, source_len)
        .reflink_block()
        .is_err()
    {
        return Ok(SnapshotCapture::Retry(snapshot));
    }
    Ok(SnapshotCapture::Captured(snapshot))
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos"
))]
pub(super) fn try_reflink_snapshot(
    source: &std::fs::File,
    destination: &Path,
    _source_len: u64,
) -> std::io::Result<SnapshotCapture> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    const CLONE_NOOWNERCOPY: u32 = 0x0002;
    let parent = destination.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "snapshot destination has no parent directory",
        )
    })?;
    let file_name = destination.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "snapshot destination has no file name",
        )
    })?;
    let file_name = CString::new(file_name.as_bytes()).map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
    })?;
    let directory = std::fs::File::open(parent)?;
    // SAFETY: the source and directory descriptors remain live for the call,
    // and file_name is a valid NUL-terminated relative path.
    let result = unsafe {
        libc::fclonefileat(
            source.as_raw_fd(),
            directory.as_raw_fd(),
            file_name.as_ptr(),
            CLONE_NOOWNERCOPY,
        )
    };
    if result == -1 {
        Ok(SnapshotCapture::Unsupported)
    } else {
        match std::fs::File::open(destination) {
            Ok(snapshot) => Ok(SnapshotCapture::Captured(snapshot)),
            Err(error) => {
                let _ = std::fs::remove_file(destination);
                Err(error)
            }
        }
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos"
)))]
pub(super) fn try_reflink_snapshot(
    _source: &std::fs::File,
    _destination: &Path,
    _source_len: u64,
) -> std::io::Result<SnapshotCapture> {
    Ok(SnapshotCapture::Unsupported)
}

pub(crate) fn snapshot_csv_source(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<SourceSnapshot> {
    snapshot_csv_source_with_capture(path, is_cancelled, &try_reflink_snapshot)
}

pub(super) fn snapshot_csv_source_with_capture(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
    try_capture: &dyn Fn(&std::fs::File, &Path, u64) -> std::io::Result<SnapshotCapture>,
) -> QuickRowsResult<SourceSnapshot> {
    check_snapshot_cancellation(is_cancelled)?;
    let mut source = std::fs::File::open(path).map_err(QuickRowsError::from)?;
    let source_metadata = source.metadata().map_err(QuickRowsError::from)?;
    if !source_metadata.is_file() {
        return Err(QuickRowsError::invalid_csv(
            "CSV source is not a regular file",
        ));
    }
    let source_len = source_metadata.len();
    let source_modified = metadata_modified(&source_metadata);
    let mut builder = tempfile::Builder::new();
    builder.prefix("quickrows-source-").suffix(".csv");
    let preferred_parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .and_then(|parent| std::fs::canonicalize(parent).ok());
    let temporary = preferred_parent
        .as_deref()
        .and_then(|parent| builder.tempfile_in(parent).ok())
        .map(Ok)
        .unwrap_or_else(|| builder.tempfile())
        .map_err(QuickRowsError::from)?;
    let (placeholder, mut placeholder_path) = temporary.into_parts();
    let temporary_path = placeholder_path.to_path_buf();
    drop(placeholder);
    std::fs::remove_file(&placeholder_path).map_err(QuickRowsError::from)?;
    // The reserved name is now absent for clonefile-style APIs. Stop the old
    // TempPath from deleting a file we do not yet own if another creator wins
    // the create-new race; a fresh cleanup guard is installed after creation.
    placeholder_path.disable_cleanup(true);
    drop(placeholder_path);

    let capture =
        try_capture(&source, &temporary_path, source_len).map_err(QuickRowsError::from)?;
    let open_stream_snapshot = |source: &mut std::fs::File| {
        source
            .seek(std::io::SeekFrom::Start(0))
            .map_err(QuickRowsError::from)?;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(QuickRowsError::from)
    };
    let (mut snapshot_file, captured) = match capture {
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            test
        ))]
        SnapshotCapture::Captured(snapshot_file) => (snapshot_file, true),
        #[cfg(any(target_os = "linux", test))]
        SnapshotCapture::Retry(snapshot_file) => {
            drop(snapshot_file);
            std::fs::remove_file(&temporary_path).map_err(QuickRowsError::from)?;
            (open_stream_snapshot(&mut source)?, false)
        }
        #[cfg(any(not(target_os = "linux"), test))]
        SnapshotCapture::Unsupported => (open_stream_snapshot(&mut source)?, false),
    };
    let cleanup_path = temporary_path.clone();
    let temporary_path = match tempfile::TempPath::try_from_path(temporary_path) {
        Ok(temporary_path) => temporary_path,
        Err(error) => {
            drop(snapshot_file);
            let _ = std::fs::remove_file(cleanup_path);
            return Err(QuickRowsError::io(error.to_string()));
        }
    };
    let (snapshot_len, content_hash) = if captured {
        hash_snapshot(&mut snapshot_file, is_cancelled)?
    } else {
        copy_snapshot(&mut source, &mut snapshot_file, is_cancelled)?
    };
    check_snapshot_cancellation(is_cancelled)?;

    // Portable stream copies use the exact observed byte sequence as their
    // capture. This metadata check rejects ordinary concurrent writes; if a
    // writer restores coarse metadata, the captured hash remains authoritative
    // for final live-source validation and later save conflict detection.
    let source_after = source.metadata().map_err(QuickRowsError::from)?;
    if snapshot_len != source_len
        || source_after.len() != source_len
        || metadata_modified(&source_after) != source_modified
    {
        return Err(QuickRowsError::source_changed(
            "CSV changed on disk while it was being captured",
        ));
    }
    let fingerprint = FileFingerprint {
        len: snapshot_len,
        modified: source_modified,
        content_hash,
    };
    Ok(SourceSnapshot {
        temporary: tempfile::NamedTempFile::from_parts(snapshot_file, temporary_path),
        fingerprint,
    })
}
