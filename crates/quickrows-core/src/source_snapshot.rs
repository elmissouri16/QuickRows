use crate::disk_cache::FileFingerprint;
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

pub(crate) struct SourceSnapshot {
    pub(crate) temporary: tempfile::NamedTempFile,
    pub(crate) fingerprint: FileFingerprint,
}

fn check_snapshot_cancellation(is_cancelled: &dyn Fn() -> bool) -> Result<(), String> {
    if is_cancelled() {
        Err("Operation cancelled".to_string())
    } else {
        Ok(())
    }
}

fn hash_snapshot(
    source: &mut std::fs::File,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<(u64, [u8; 32]), String> {
    let mut hasher = blake3::Hasher::new();
    let mut hashed = 0u64;
    let mut buffer = vec![0u8; SOURCE_SNAPSHOT_BUFFER_BYTES];
    loop {
        check_snapshot_cancellation(is_cancelled)?;
        let read = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        hashed = hashed.saturating_add(read as u64);
    }
    Ok((hashed, *hasher.finalize().as_bytes()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows { volume_serial: u32, file_id: u64 },
}

fn file_identity(file: &std::fs::File) -> Result<Option<FileIdentity>, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        Ok(Some(FileIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        }))
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: the call receives a live File handle and a correctly sized,
        // initialized output structure for the duration of the call.
        let result = unsafe {
            GetFileInformationByHandle(
                file.as_raw_handle() as _,
                std::ptr::from_mut(&mut information),
            )
        };
        if result == 0 {
            return Ok(None);
        }
        let volume_serial = information.dwVolumeSerialNumber;
        let file_id =
            u64::from(information.nFileIndexHigh) << 32 | u64::from(information.nFileIndexLow);
        if volume_serial == 0 && file_id == 0 {
            Ok(None)
        } else {
            Ok(Some(FileIdentity::Windows {
                volume_serial,
                file_id,
            }))
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        Ok(None)
    }
}

pub(crate) fn verify_path_references_open_file(
    path: &Path,
    expected: &std::fs::File,
) -> Result<(), String> {
    let current = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let expected_identity = file_identity(expected)?;
    let current_identity = file_identity(&current)?;
    match (expected_identity, current_identity) {
        (Some(expected), Some(current)) if expected == current => Ok(()),
        (Some(_), Some(_)) => {
            Err("Saved CSV temporary file was replaced before commit".to_string())
        }
        _ => Err("Could not verify saved CSV temporary file identity".to_string()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileChangeToken {
    #[cfg(unix)]
    Unix { seconds: i64, nanoseconds: i64 },
    #[cfg(windows)]
    Windows(i64),
}

fn file_change_token(file: &std::fs::File) -> Result<Option<FileChangeToken>, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        Ok(Some(FileChangeToken::Unix {
            seconds: metadata.ctime(),
            nanoseconds: metadata.ctime_nsec(),
        }))
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            FileBasicInfo, GetFileInformationByHandleEx, FILE_BASIC_INFO,
        };

        let mut information = FILE_BASIC_INFO::default();
        // SAFETY: the call receives a live File handle and a correctly sized,
        // initialized FILE_BASIC_INFO output buffer for the duration of the call.
        let result = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle() as _,
                FileBasicInfo,
                std::ptr::from_mut(&mut information).cast(),
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        return Ok((result != 0 && information.ChangeTime != 0)
            .then_some(FileChangeToken::Windows(information.ChangeTime)));
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        Ok(None)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpenFileState {
    identity: FileIdentity,
    change: FileChangeToken,
    len: u64,
    modified: u64,
}

impl OpenFileState {
    pub(crate) fn fingerprint(
        self,
        expected_len: u64,
        content_hash: [u8; 32],
    ) -> Result<FileFingerprint, String> {
        if self.len != expected_len {
            return Err("Saved CSV length changed after serialization".to_string());
        }
        Ok(FileFingerprint {
            len: self.len,
            modified: self.modified,
            content_hash,
        })
    }
}

pub(crate) fn capture_open_file_state(file: &std::fs::File) -> Result<OpenFileState, String> {
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("Saved CSV candidate is not a regular file".to_string());
    }
    Ok(OpenFileState {
        identity: file_identity(file)?
            .ok_or_else(|| "Could not verify saved CSV temporary file identity".to_string())?,
        change: file_change_token(file)?
            .ok_or_else(|| "Could not verify saved CSV temporary file changes".to_string())?,
        len: metadata.len(),
        modified: metadata_modified(&metadata),
    })
}

pub(crate) fn verify_open_file_state(
    file: &std::fs::File,
    expected: OpenFileState,
) -> Result<(), String> {
    if capture_open_file_state(file)? == expected {
        Ok(())
    } else {
        Err("Saved CSV candidate changed before commit".to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChangeObservation {
    Stable,
    Changed,
    Unavailable,
}

struct ObservedFingerprint {
    fingerprint: FileFingerprint,
    change: ChangeObservation,
    terminal_change: Option<FileChangeToken>,
}

fn fingerprint_open_file(
    source: &mut std::fs::File,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<ObservedFingerprint, String> {
    let metadata = source.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("CSV source is not a regular file".to_string());
    }
    let expected_len = metadata.len();
    let modified = metadata_modified(&metadata);
    let change_before = file_change_token(source)?;
    let (len, content_hash) = hash_snapshot(source, is_cancelled)?;
    check_snapshot_cancellation(is_cancelled)?;
    let metadata_after = source.metadata().map_err(|error| error.to_string())?;
    let change_after = file_change_token(source)?;
    if len != expected_len
        || metadata_after.len() != expected_len
        || metadata_modified(&metadata_after) != modified
    {
        return Err("CSV changed on disk while it was being fingerprinted".to_string());
    }
    let change = match (change_before, change_after) {
        (Some(before), Some(after)) if before == after => ChangeObservation::Stable,
        (Some(_), Some(_)) => ChangeObservation::Changed,
        _ => ChangeObservation::Unavailable,
    };
    Ok(ObservedFingerprint {
        fingerprint: FileFingerprint {
            len,
            modified,
            content_hash,
        },
        change,
        terminal_change: change_after,
    })
}

fn file_fingerprint_with_identity(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
    identity_of: &dyn Fn(&std::fs::File) -> Result<Option<FileIdentity>, String>,
) -> Result<FileFingerprint, String> {
    check_snapshot_cancellation(is_cancelled)?;
    let mut source = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let identity = identity_of(&source)?;
    let observed = fingerprint_open_file(&mut source, is_cancelled)?;
    let fingerprint = observed.fingerprint;
    check_snapshot_cancellation(is_cancelled)?;

    // Reopen the path after hashing so a replacement or symlink retarget cannot
    // hide behind the still-live descriptor for the original referent.
    let mut current = std::fs::File::open(path)
        .map_err(|_| "CSV changed on disk while it was being fingerprinted".to_string())?;
    let current_metadata = current.metadata().map_err(|error| error.to_string())?;
    if current_metadata.len() != fingerprint.len
        || metadata_modified(&current_metadata) != fingerprint.modified
    {
        return Err("CSV changed on disk while it was being fingerprinted".to_string());
    }
    let current_identity = identity_of(&current)?;
    check_snapshot_cancellation(is_cancelled)?;
    let current_change = file_change_token(&current)?;
    let identity_is_stable = match (identity, current_identity) {
        (Some(expected), Some(actual)) if expected == actual => true,
        (Some(_), Some(_)) => {
            return Err("CSV changed on disk while it was being fingerprinted".to_string());
        }
        _ => false,
    };
    let terminal_change_is_stable = matches!(
        (observed.terminal_change, current_change),
        (Some(expected), Some(actual)) if expected == actual
    );
    if identity_is_stable
        && observed.change == ChangeObservation::Stable
        && terminal_change_is_stable
    {
        // This is the final filesystem observation. A writer can always race a
        // later change unless the source is locked for the entire open.
        return Ok(fingerprint);
    }

    // A write/change token or unavailable identity is only a dirty signal, not
    // proof of a content conflict. Rehash the reopened path to avoid false
    // rejections for chmod/ACL changes while retaining a conservative fallback.
    let current_observed = fingerprint_open_file(&mut current, is_cancelled)?;
    if current_observed.change == ChangeObservation::Changed
        || current_observed.fingerprint != fingerprint
    {
        return Err("CSV changed on disk while it was being fingerprinted".to_string());
    }

    // Validate the fallback handle against the path again so a retarget during
    // its hash cannot hide behind that descriptor either.
    let mut latest = std::fs::File::open(path)
        .map_err(|_| "CSV changed on disk while it was being fingerprinted".to_string())?;
    let latest_metadata = latest.metadata().map_err(|error| error.to_string())?;
    if latest_metadata.len() != fingerprint.len
        || metadata_modified(&latest_metadata) != fingerprint.modified
    {
        return Err("CSV changed on disk while it was being fingerprinted".to_string());
    }
    let latest_identity = identity_of(&latest)?;
    check_snapshot_cancellation(is_cancelled)?;
    let latest_change = file_change_token(&latest)?;
    match (current_identity, latest_identity) {
        (Some(expected), Some(actual)) if expected != actual => {
            return Err("CSV changed on disk while it was being fingerprinted".to_string());
        }
        (Some(_), Some(_))
            if current_observed.change == ChangeObservation::Stable
                && matches!(
                    (current_observed.terminal_change, latest_change),
                    (Some(expected), Some(actual)) if expected == actual
                ) =>
        {
            return Ok(fingerprint);
        }
        _ => {}
    }

    // Identity/change information is unavailable or remained dirty. Bound the
    // validation chain with one final hash of the latest reopened referent;
    // available change tokens reject mutation during that pass. Without a
    // whole-open lock, changes after this final observation remain inherently
    // the responsibility of watcher/save-conflict handling.
    let latest_observed = fingerprint_open_file(&mut latest, is_cancelled)?;
    if latest_observed.change == ChangeObservation::Changed
        || latest_observed.fingerprint != fingerprint
    {
        Err("CSV changed on disk while it was being fingerprinted".to_string())
    } else {
        Ok(fingerprint)
    }
}

fn file_fingerprint_with_cancellation(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<FileFingerprint, String> {
    file_fingerprint_with_identity(path, is_cancelled, &file_identity)
}

pub(crate) fn file_fingerprint_cancellable(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<FileFingerprint, String> {
    file_fingerprint_with_cancellation(path, is_cancelled)
}

fn copy_snapshot(
    source: &mut std::fs::File,
    destination: &mut std::fs::File,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<(u64, [u8; 32]), String> {
    let mut hasher = blake3::Hasher::new();
    let mut copied = 0u64;
    let mut buffer = vec![0u8; SOURCE_SNAPSHOT_BUFFER_BYTES];
    loop {
        check_snapshot_cancellation(is_cancelled)?;
        let read = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        copied = copied.saturating_add(read as u64);
        destination
            .write_all(&buffer[..read])
            .map_err(|error| error.to_string())?;
    }
    destination.flush().map_err(|error| error.to_string())?;
    Ok((copied, *hasher.finalize().as_bytes()))
}

// A failed capture returns its created file through Retry so the caller can
// remove only the destination it owns before starting the stream fallback.
enum SnapshotCapture {
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
fn try_reflink_snapshot(
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
fn try_reflink_snapshot(
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
fn try_reflink_snapshot(
    _source: &std::fs::File,
    _destination: &Path,
    _source_len: u64,
) -> std::io::Result<SnapshotCapture> {
    Ok(SnapshotCapture::Unsupported)
}

pub(crate) fn snapshot_csv_source(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
) -> Result<SourceSnapshot, String> {
    snapshot_csv_source_with_capture(path, is_cancelled, &try_reflink_snapshot)
}

fn snapshot_csv_source_with_capture(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
    try_capture: &dyn Fn(&std::fs::File, &Path, u64) -> std::io::Result<SnapshotCapture>,
) -> Result<SourceSnapshot, String> {
    check_snapshot_cancellation(is_cancelled)?;
    let mut source = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let source_metadata = source.metadata().map_err(|error| error.to_string())?;
    if !source_metadata.is_file() {
        return Err("CSV source is not a regular file".to_string());
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
        .map_err(|error| error.to_string())?;
    let (placeholder, mut placeholder_path) = temporary.into_parts();
    let temporary_path = placeholder_path.to_path_buf();
    drop(placeholder);
    std::fs::remove_file(&placeholder_path).map_err(|error| error.to_string())?;
    // The reserved name is now absent for clonefile-style APIs. Stop the old
    // TempPath from deleting a file we do not yet own if another creator wins
    // the create-new race; a fresh cleanup guard is installed after creation.
    placeholder_path.disable_cleanup(true);
    drop(placeholder_path);

    let capture =
        try_capture(&source, &temporary_path, source_len).map_err(|error| error.to_string())?;
    let open_stream_snapshot = |source: &mut std::fs::File| {
        source
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|error| error.to_string())?;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|error| error.to_string())
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
            std::fs::remove_file(&temporary_path).map_err(|error| error.to_string())?;
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
            return Err(error.to_string());
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
    let source_after = source.metadata().map_err(|error| error.to_string())?;
    if snapshot_len != source_len
        || source_after.len() != source_len
        || metadata_modified(&source_after) != source_modified
    {
        return Err("CSV changed on disk while it was being captured".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::file_fingerprint;
    use std::path::PathBuf;

    #[test]
    fn open_file_identity_rejects_replaced_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("candidate.csv");
        std::fs::write(&path, "a,b\n1,2\n").unwrap();
        let original = std::fs::File::open(&path).unwrap();
        verify_path_references_open_file(&path, &original).unwrap();

        let replacement = dir.path().join("replacement.csv");
        std::fs::write(&replacement, "a,b\n3,4\n").unwrap();

        assert!(verify_path_references_open_file(&replacement, &original)
            .unwrap_err()
            .contains("replaced"));
    }

    #[test]
    fn open_file_state_rejects_a_same_length_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("candidate.csv");
        std::fs::write(&path, "a,b\n1,2\n").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let state = capture_open_file_state(&file).unwrap();

        std::fs::write(&path, "a,b\n3,4\n").unwrap();

        assert!(verify_open_file_state(&file, state)
            .unwrap_err()
            .contains("changed"));
    }

    #[test]
    fn snapshot_copy_hashes_multiple_buffers_and_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.csv");
        let destination_path = dir.path().join("destination.csv");
        let bytes = (0..SOURCE_SNAPSHOT_BUFFER_BYTES * 2 + 137)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&source_path, &bytes).unwrap();
        let mut source = std::fs::File::open(&source_path).unwrap();
        let mut destination = std::fs::File::create(&destination_path).unwrap();

        let (copied, content_hash) =
            copy_snapshot(&mut source, &mut destination, &|| false).unwrap();
        drop(destination);

        assert_eq!(copied, bytes.len() as u64);
        assert_eq!(content_hash, *blake3::hash(&bytes).as_bytes());
        assert_eq!(std::fs::read(destination_path).unwrap(), bytes);
    }

    #[test]
    fn failed_capture_with_owned_destination_falls_back_to_stream_copy() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.csv");
        let bytes = vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES + 17];
        std::fs::write(&source_path, &bytes).unwrap();
        let capture = |_: &std::fs::File, destination: &Path, _: u64| {
            let snapshot = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(destination)?;
            Ok(SnapshotCapture::Retry(snapshot))
        };

        let snapshot = snapshot_csv_source_with_capture(&source_path, &|| false, &capture).unwrap();

        assert_eq!(std::fs::read(snapshot.temporary.path()).unwrap(), bytes);
        assert_eq!(
            snapshot.fingerprint.content_hash,
            *blake3::hash(&bytes).as_bytes()
        );
        drop(snapshot);
        assert!(snapshot_artifacts(dir.path()).is_empty());
    }

    #[test]
    fn stream_capture_rejects_a_detectable_change_and_removes_the_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.csv");
        let bytes = vec![b'a'; SOURCE_SNAPSHOT_BUFFER_BYTES * 2 + 17];
        std::fs::write(&source_path, &bytes).unwrap();
        let original_modified = std::fs::metadata(&source_path).unwrap().modified().unwrap();
        let changed_modified = original_modified
            .checked_add(std::time::Duration::from_secs(60))
            .unwrap();
        let checks = std::cell::Cell::new(0usize);
        let changed = std::cell::Cell::new(false);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 2 {
                let mut replacement = bytes.clone();
                replacement[..SOURCE_SNAPSHOT_BUFFER_BYTES].fill(b'z');
                std::fs::write(&source_path, replacement).unwrap();
                let source = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&source_path)
                    .unwrap();
                source
                    .set_times(std::fs::FileTimes::new().set_modified(changed_modified))
                    .unwrap();
                changed.set(true);
            }
            false
        };
        let force_copy = |_: &std::fs::File, _: &Path, _: u64| Ok(SnapshotCapture::Unsupported);

        let error = snapshot_csv_source_with_capture(&source_path, &is_cancelled, &force_copy)
            .err()
            .expect("capture should reject a detectable source change");

        assert!(changed.get());
        assert_eq!(error, "CSV changed on disk while it was being captured");
        assert!(snapshot_artifacts(dir.path()).is_empty());
    }

    #[test]
    fn stream_capture_fingerprints_exact_observed_bytes_when_mtime_is_restored() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.csv");
        let mut original = vec![b'a'; SOURCE_SNAPSHOT_BUFFER_BYTES];
        original.extend(vec![b'b'; SOURCE_SNAPSHOT_BUFFER_BYTES]);
        original.extend(vec![b'c'; 17]);
        std::fs::write(&source_path, &original).unwrap();
        let original_modified = std::fs::metadata(&source_path).unwrap().modified().unwrap();
        let mut replacement = original.clone();
        replacement[..SOURCE_SNAPSHOT_BUFFER_BYTES].fill(b'z');
        replacement[SOURCE_SNAPSHOT_BUFFER_BYTES..SOURCE_SNAPSHOT_BUFFER_BYTES * 2].fill(b'y');
        let checks = std::cell::Cell::new(0usize);
        let changed = std::cell::Cell::new(false);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 2 {
                std::fs::write(&source_path, &replacement).unwrap();
                let source = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&source_path)
                    .unwrap();
                source
                    .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                    .unwrap();
                changed.set(true);
            }
            false
        };
        let force_copy = |_: &std::fs::File, _: &Path, _: u64| Ok(SnapshotCapture::Unsupported);

        let snapshot =
            snapshot_csv_source_with_capture(&source_path, &is_cancelled, &force_copy).unwrap();
        let captured = std::fs::read(snapshot.temporary.path()).unwrap();
        let mut observed = original;
        observed[SOURCE_SNAPSHOT_BUFFER_BYTES..SOURCE_SNAPSHOT_BUFFER_BYTES * 2].fill(b'y');

        assert!(changed.get());
        assert_eq!(captured, observed);
        assert_eq!(snapshot.fingerprint.len, captured.len() as u64);
        assert_eq!(
            snapshot.fingerprint.content_hash,
            *blake3::hash(&captured).as_bytes()
        );
        assert_ne!(
            snapshot.fingerprint,
            file_fingerprint(&source_path).unwrap()
        );
    }

    fn snapshot_artifacts(directory: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(directory)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("quickrows-source-"))
            })
            .collect()
    }

    #[test]
    fn cancelled_snapshot_copy_removes_the_partial_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.csv");
        std::fs::write(&source_path, vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 4]).unwrap();
        let checks = std::cell::Cell::new(0usize);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            current >= 3
        };
        let force_copy = |_: &std::fs::File, _: &Path, _: u64| Ok(SnapshotCapture::Unsupported);

        let error = snapshot_csv_source_with_capture(&source_path, &is_cancelled, &force_copy)
            .err()
            .expect("stream capture should be cancelled");

        assert_eq!(error, "Operation cancelled");
        assert!(checks.get() >= 4);
        assert!(snapshot_artifacts(dir.path()).is_empty());
    }

    #[test]
    fn cancelled_snapshot_hash_removes_the_captured_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.csv");
        std::fs::write(&source_path, vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 4]).unwrap();
        let checks = std::cell::Cell::new(0usize);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            current >= 3
        };
        let capture = |source: &std::fs::File, destination: &Path, _: u64| {
            let mut source = source.try_clone()?;
            source.seek(std::io::SeekFrom::Start(0))?;
            let mut snapshot = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(destination)?;
            std::io::copy(&mut source, &mut snapshot)?;
            snapshot.seek(std::io::SeekFrom::Start(0))?;
            Ok(SnapshotCapture::Captured(snapshot))
        };

        let error = snapshot_csv_source_with_capture(&source_path, &is_cancelled, &capture)
            .err()
            .expect("captured-file hashing should be cancelled");

        assert_eq!(error, "Operation cancelled");
        assert!(checks.get() >= 4);
        assert!(snapshot_artifacts(dir.path()).is_empty());
    }

    #[test]
    fn source_validation_detects_same_length_change_with_restored_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.csv");
        let original = b"name,value\nalpha,1\n";
        let replacement = b"name,value\nbravo,9\n";
        assert_eq!(original.len(), replacement.len());
        std::fs::write(&path, original).unwrap();
        let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let snapshot = snapshot_csv_source(&path, &|| false).unwrap();

        std::fs::write(&path, replacement).unwrap();
        let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        source
            .set_times(std::fs::FileTimes::new().set_modified(original_modified))
            .unwrap();
        drop(source);

        let replacement_fingerprint = file_fingerprint(&path).unwrap();
        assert_eq!(
            replacement_fingerprint,
            file_fingerprint_cancellable(&path, &|| false).unwrap()
        );
        assert_eq!(replacement_fingerprint.len, snapshot.fingerprint.len);
        assert_eq!(
            replacement_fingerprint.modified,
            snapshot.fingerprint.modified
        );
        assert_ne!(
            replacement_fingerprint.content_hash,
            snapshot.fingerprint.content_hash
        );
        assert_eq!(std::fs::read(snapshot.temporary.path()).unwrap(), original);
        assert_eq!(
            snapshot.fingerprint.content_hash,
            *blake3::hash(original).as_bytes()
        );
    }

    #[test]
    fn cancelled_final_live_fingerprint_stops_between_buffers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.csv");
        std::fs::write(&path, vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 4]).unwrap();
        let snapshot = snapshot_csv_source(&path, &|| false).unwrap();
        let checks = std::cell::Cell::new(0usize);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            current >= 3
        };

        let error = file_fingerprint_with_cancellation(&path, &is_cancelled)
            .expect_err("live fingerprint should be cancelled");

        assert_eq!(error, "Operation cancelled");
        assert!(checks.get() >= 4);
        drop(snapshot);
        assert!(snapshot_artifacts(dir.path()).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn final_live_fingerprint_detects_restored_mtime_write_behind_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.csv");
        let original = vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 3 + 17];
        std::fs::write(&path, &original).unwrap();
        let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut replacement = original.clone();
        replacement[..SOURCE_SNAPSHOT_BUFFER_BYTES].fill(b'z');
        let checks = std::cell::Cell::new(0usize);
        let changed = std::cell::Cell::new(false);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 2 {
                std::fs::write(&path, &replacement).unwrap();
                let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                source
                    .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                    .unwrap();
                changed.set(true);
            }
            false
        };

        let error = file_fingerprint_with_cancellation(&path, &is_cancelled)
            .expect_err("live fingerprint should reject a write behind its read cursor");

        assert!(changed.get());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            original.len() as u64
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            original_modified
        );
        assert!(error.contains("changed on disk while it was being fingerprinted"));
    }

    #[cfg(unix)]
    #[test]
    fn final_live_fingerprint_detects_write_after_hash_before_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.csv");
        let original = vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 2];
        std::fs::write(&path, &original).unwrap();
        let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut replacement = original;
        replacement[..SOURCE_SNAPSHOT_BUFFER_BYTES].fill(b'z');
        let checks = std::cell::Cell::new(0usize);
        let changed = std::cell::Cell::new(false);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 5 {
                std::fs::write(&path, &replacement).unwrap();
                let source = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                source
                    .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                    .unwrap();
                changed.set(true);
            }
            false
        };

        let error = file_fingerprint_with_cancellation(&path, &is_cancelled)
            .expect_err("live fingerprint should reject a post-hash write");

        assert!(changed.get());
        assert!(error.contains("changed on disk while it was being fingerprinted"));
    }

    #[cfg(unix)]
    #[test]
    fn identity_unavailable_fallback_rehashes_a_retargeted_path() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        let link = dir.path().join("linked.csv");
        let first_bytes = vec![b'a'; SOURCE_SNAPSHOT_BUFFER_BYTES * 3 + 17];
        let second_bytes = vec![b'b'; first_bytes.len()];
        std::fs::write(&first, &first_bytes).unwrap();
        std::fs::write(&second, &second_bytes).unwrap();
        let first_modified = std::fs::metadata(&first).unwrap().modified().unwrap();
        let second_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&second)
            .unwrap();
        second_file
            .set_times(std::fs::FileTimes::new().set_modified(first_modified))
            .unwrap();
        drop(second_file);
        assert_eq!(
            metadata_modified(&std::fs::metadata(&first).unwrap()),
            metadata_modified(&std::fs::metadata(&second).unwrap())
        );
        symlink(&first, &link).unwrap();
        let checks = std::cell::Cell::new(0usize);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 2 {
                std::fs::remove_file(&link).unwrap();
                symlink(&second, &link).unwrap();
            }
            false
        };
        let no_identity = |_: &std::fs::File| Ok(None);

        let error = file_fingerprint_with_identity(&link, &is_cancelled, &no_identity)
            .expect_err("identity fallback should reject different live content");

        assert!(error.contains("changed on disk while it was being fingerprinted"));
    }

    #[cfg(unix)]
    #[test]
    fn identity_fallback_rechecks_path_after_its_rehash() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        let link = dir.path().join("linked.csv");
        let first_bytes = vec![b'a'; SOURCE_SNAPSHOT_BUFFER_BYTES * 2];
        let second_bytes = vec![b'b'; first_bytes.len()];
        std::fs::write(&first, &first_bytes).unwrap();
        std::fs::write(&second, &second_bytes).unwrap();
        let first_modified = std::fs::metadata(&first).unwrap().modified().unwrap();
        let second_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&second)
            .unwrap();
        second_file
            .set_times(std::fs::FileTimes::new().set_modified(first_modified))
            .unwrap();
        drop(second_file);
        assert_eq!(
            metadata_modified(&std::fs::metadata(&first).unwrap()),
            metadata_modified(&std::fs::metadata(&second).unwrap())
        );
        symlink(&first, &link).unwrap();
        let checks = std::cell::Cell::new(0usize);
        let retargeted = std::cell::Cell::new(false);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 8 {
                std::fs::remove_file(&link).unwrap();
                symlink(&second, &link).unwrap();
                retargeted.set(true);
            }
            false
        };
        let no_identity = |_: &std::fs::File| Ok(None);

        let error = file_fingerprint_with_identity(&link, &is_cancelled, &no_identity)
            .expect_err("fallback should reject a retarget during its hash");

        assert!(retargeted.get());
        assert!(error.contains("changed on disk while it was being fingerprinted"));
    }

    #[cfg(unix)]
    #[test]
    fn final_live_fingerprint_detects_retarget_during_hashing() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        let link = dir.path().join("linked.csv");
        let bytes = vec![b'x'; SOURCE_SNAPSHOT_BUFFER_BYTES * 3 + 17];
        std::fs::write(&first, &bytes).unwrap();
        std::fs::write(&second, &bytes).unwrap();
        let first_modified = std::fs::metadata(&first).unwrap().modified().unwrap();
        let second_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&second)
            .unwrap();
        second_file
            .set_times(std::fs::FileTimes::new().set_modified(first_modified))
            .unwrap();
        drop(second_file);
        assert_eq!(
            metadata_modified(&std::fs::metadata(&first).unwrap()),
            metadata_modified(&std::fs::metadata(&second).unwrap())
        );
        symlink(&first, &link).unwrap();
        let checks = std::cell::Cell::new(0usize);
        let retargeted = std::cell::Cell::new(false);
        let is_cancelled = || {
            let current = checks.get();
            checks.set(current + 1);
            if current == 2 {
                std::fs::remove_file(&link).unwrap();
                symlink(&second, &link).unwrap();
                retargeted.set(true);
            }
            false
        };

        let error = file_fingerprint_with_cancellation(&link, &is_cancelled)
            .expect_err("live fingerprint should reject a new path referent");

        assert!(retargeted.get());
        assert!(error.contains("changed on disk while it was being fingerprinted"));
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_capture_uses_the_open_referent_through_a_symlink_aba() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        let link = dir.path().join("linked.csv");
        std::fs::write(&first, "name,value\nfirst,1\n").unwrap();
        std::fs::write(&second, "name,value\nother,2\n").unwrap();
        symlink(&first, &link).unwrap();
        let capture = |source: &std::fs::File, destination: &Path, len: u64| {
            std::fs::remove_file(&link)?;
            symlink(&second, &link)?;
            let result = try_reflink_snapshot(source, destination, len);
            std::fs::remove_file(&link)?;
            symlink(&first, &link)?;
            result
        };

        let snapshot = snapshot_csv_source_with_capture(&link, &|| false, &capture).unwrap();

        assert_eq!(
            std::fs::read(snapshot.temporary.path()).unwrap(),
            std::fs::read(first).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_temporary_is_anchored_across_parent_symlink_aba() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first_parent = dir.path().join("first-parent");
        let second_parent = dir.path().join("second-parent");
        let parent_link = dir.path().join("parent-link");
        std::fs::create_dir(&first_parent).unwrap();
        std::fs::create_dir(&second_parent).unwrap();
        std::fs::write(first_parent.join("source.csv"), "name,value\nfirst,1\n").unwrap();
        std::fs::write(second_parent.join("source.csv"), "name,value\nother,2\n").unwrap();
        symlink(&first_parent, &parent_link).unwrap();
        let path = parent_link.join("source.csv");
        let capture = |source: &std::fs::File, destination: &Path, _: u64| {
            std::fs::remove_file(&parent_link)?;
            symlink(&second_parent, &parent_link)?;
            let mut source = source.try_clone()?;
            source.seek(std::io::SeekFrom::Start(0))?;
            let mut snapshot = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(destination)?;
            std::io::copy(&mut source, &mut snapshot)?;
            snapshot.seek(std::io::SeekFrom::Start(0))?;
            std::fs::remove_file(&parent_link)?;
            symlink(&first_parent, &parent_link)?;
            Ok(SnapshotCapture::Captured(snapshot))
        };

        let snapshot = snapshot_csv_source_with_capture(&path, &|| false, &capture).unwrap();
        let snapshot_path = snapshot.temporary.path().to_path_buf();
        let snapshot_name = snapshot_path.file_name().unwrap().to_owned();

        assert_eq!(
            snapshot_path.parent(),
            Some(std::fs::canonicalize(&first_parent).unwrap().as_path())
        );
        assert_eq!(
            std::fs::read(&snapshot_path).unwrap(),
            std::fs::read(first_parent.join("source.csv")).unwrap()
        );
        assert!(!second_parent.join(&snapshot_name).exists());
        drop(snapshot);
        assert!(!first_parent.join(&snapshot_name).exists());
        assert!(!second_parent.join(&snapshot_name).exists());
    }
}
