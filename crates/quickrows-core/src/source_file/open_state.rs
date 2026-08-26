use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows { volume_serial: u32, file_id: u64 },
}

pub(super) fn file_identity(file: &std::fs::File) -> QuickRowsResult<Option<FileIdentity>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(QuickRowsError::from)?;
        Ok(Some(FileIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        }))
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
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
) -> QuickRowsResult<()> {
    let current = std::fs::File::open(path).map_err(QuickRowsError::from)?;
    let expected_identity = file_identity(expected)?;
    let current_identity = file_identity(&current)?;
    match (expected_identity, current_identity) {
        (Some(expected), Some(current)) if expected == current => Ok(()),
        (Some(_), Some(_)) => Err(QuickRowsError::destination_changed(
            "Saved CSV temporary file was replaced before commit",
        )),
        _ => Err(QuickRowsError::destination_changed(
            "Could not verify saved CSV temporary file identity",
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileChangeToken {
    #[cfg(unix)]
    Unix { seconds: i64, nanoseconds: i64 },
    #[cfg(windows)]
    Windows(i64),
}

pub(super) fn file_change_token(file: &std::fs::File) -> QuickRowsResult<Option<FileChangeToken>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(QuickRowsError::from)?;
        Ok(Some(FileChangeToken::Unix {
            seconds: metadata.ctime(),
            nanoseconds: metadata.ctime_nsec(),
        }))
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandleEx,
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
    ) -> QuickRowsResult<FileFingerprint> {
        if self.len != expected_len {
            return Err(QuickRowsError::destination_changed(
                "Saved CSV length changed after serialization",
            ));
        }
        Ok(FileFingerprint {
            len: self.len,
            modified: self.modified,
            content_hash,
        })
    }
}

pub(crate) fn capture_open_file_state(file: &std::fs::File) -> QuickRowsResult<OpenFileState> {
    let metadata = file.metadata().map_err(QuickRowsError::from)?;
    if !metadata.is_file() {
        return Err(QuickRowsError::destination_changed(
            "Saved CSV candidate is not a regular file",
        ));
    }
    Ok(OpenFileState {
        identity: file_identity(file)?.ok_or_else(|| {
            QuickRowsError::destination_changed(
                "Could not verify saved CSV temporary file identity",
            )
        })?,
        change: file_change_token(file)?.ok_or_else(|| {
            QuickRowsError::destination_changed("Could not verify saved CSV temporary file changes")
        })?,
        len: metadata.len(),
        modified: metadata_modified(&metadata),
    })
}

pub(crate) fn verify_open_file_state(
    file: &std::fs::File,
    expected: OpenFileState,
) -> QuickRowsResult<()> {
    if capture_open_file_state(file)? == expected {
        Ok(())
    } else {
        Err(QuickRowsError::destination_changed(
            "Saved CSV candidate changed before commit",
        ))
    }
}
