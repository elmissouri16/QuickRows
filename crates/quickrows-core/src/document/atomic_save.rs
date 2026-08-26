#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DestinationState {
    Missing,
    Existing(FileFingerprint),
}

fn resolve_save_target(path: &Path) -> QuickRowsResult<PathBuf> {
    fn resolve(path: &Path, remaining_links: usize) -> QuickRowsResult<PathBuf> {
        if remaining_links == 0 {
            return Err(QuickRowsError::io(
                "CSV destination contains too many symbolic links",
            ));
        }
        if let Ok(canonical) = std::fs::canonicalize(path) {
            return Ok(canonical);
        }
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let link = std::fs::read_link(path).map_err(QuickRowsError::from)?;
                let linked_path = if link.is_absolute() {
                    link
                } else {
                    path.parent()
                        .filter(|parent| !parent.as_os_str().is_empty())
                        .unwrap_or_else(|| Path::new("."))
                        .join(link)
                };
                resolve(&linked_path, remaining_links - 1)
            }
            Ok(_) => std::fs::canonicalize(path).map_err(QuickRowsError::from),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let file_name = path
                    .file_name()
                    .ok_or_else(|| "CSV destination has no file name".to_string())?;
                let parent = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                Ok(resolve(parent, remaining_links - 1)?.join(file_name))
            }
            Err(error) => Err(QuickRowsError::from(error)),
        }
    }

    resolve(path, 40)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn exchange_paths(first: &Path, second: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    #[cfg(target_os = "macos")]
    use std::os::raw::{c_char, c_int, c_uint};
    use std::os::unix::ffi::OsStrExt;

    let first = CString::new(first.as_os_str().as_bytes())?;
    let second = CString::new(second.as_os_str().as_bytes())?;
    #[cfg(target_os = "linux")]
    unsafe {
        if libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            first.as_ptr(),
            libc::AT_FDCWD,
            second.as_ptr(),
            libc::RENAME_EXCHANGE,
        ) == 0
        {
            return Ok(());
        }
    }
    #[cfg(target_os = "macos")]
    unsafe {
        unsafe extern "C" {
            fn renamex_np(old: *const c_char, new: *const c_char, flags: c_uint) -> c_int;
        }
        const RENAME_SWAP: c_uint = 2;
        if renamex_np(first.as_ptr(), second.as_ptr(), RENAME_SWAP) == 0 {
            return Ok(());
        }
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(22) | Some(38) | Some(45) | Some(95)
    ) {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, error))
    } else {
        Err(error)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn exchange_paths(_first: &Path, _second: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic file exchange is unavailable on this platform",
    ))
}

#[cfg(windows)]
fn replace_file_windows(target: &Path, replacement: &Path, backup: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{ReplaceFileW, REPLACEFILE_WRITE_THROUGH};

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let target = wide(target);
    let replacement = wide(replacement);
    let backup = wide(backup);
    let replaced = unsafe {
        ReplaceFileW(
            target.as_ptr(),
            replacement.as_ptr(),
            backup.as_ptr(),
            REPLACEFILE_WRITE_THROUGH,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn unused_temporary_path(parent: &Path, prefix: &str) -> QuickRowsResult<tempfile::TempPath> {
    let path = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(QuickRowsError::from)?
        .into_temp_path();
    std::fs::remove_file(&path).map_err(QuickRowsError::from)?;
    Ok(path)
}

#[cfg(windows)]
fn commit_temporary_windows(
    temporary: tempfile::NamedTempFile,
    logical_target: &Path,
    resolved_target: &Path,
    expected: DestinationState,
    replacement_fingerprint: FileFingerprint,
) -> QuickRowsResult<()> {
    let conflict = || {
        QuickRowsError::destination_changed(
            "CSV changed on disk; save was cancelled to protect the external changes",
        )
    };
    let parent = resolved_target.parent().unwrap_or_else(|| Path::new("."));
    let backup = unused_temporary_path(parent, ".quickrows-backup-")?;
    let recovery = unused_temporary_path(parent, ".quickrows-recovery-")?;
    // ReplaceFileW requires the replacement path to be movable. Consume the
    // NamedTempFile first so its open handle cannot deny delete sharing.
    let temporary = temporary.into_temp_path();
    replace_file_windows(resolved_target, &temporary, &backup).map_err(QuickRowsError::from)?;

    let route_matches =
        resolve_save_target(logical_target).is_ok_and(|current| current == resolved_target);
    let displaced_matches = destination_state(&backup) == Ok(expected);
    let replacement_matches = destination_state(resolved_target)
        == Ok(DestinationState::Existing(replacement_fingerprint));
    if route_matches && displaced_matches && replacement_matches {
        drop(backup);
        drop(temporary);
        return Ok(());
    }

    if let Err(rollback_error) = replace_file_windows(resolved_target, &backup, &recovery) {
        let backup = backup.keep().map_err(|error| QuickRowsError::from(error.error))?;
        return Err(QuickRowsError::destination_changed(format!(
            "CSV changed during save and rollback failed ({rollback_error}); the displaced destination is at {}",
            backup.display()
        )));
    }
    drop(backup);
    drop(temporary);
    let recovery = recovery.keep().map_err(|error| QuickRowsError::from(error.error))?;
    Err(QuickRowsError::destination_changed(format!(
        "{}; the uncommitted file was preserved at {}",
        conflict(),
        recovery.display()
    )))
}

fn commit_temporary(
    temporary: tempfile::NamedTempFile,
    logical_target: &Path,
    resolved_target: &Path,
    expected: DestinationState,
    replacement_fingerprint: FileFingerprint,
    replacement_state: OpenFileState,
) -> QuickRowsResult<()> {
    let conflict = || {
        QuickRowsError::destination_changed(
            "CSV changed on disk; save was cancelled to protect the external changes",
        )
    };
    let route_matches =
        || resolve_save_target(logical_target).is_ok_and(|current| current == resolved_target);
    if !route_matches() {
        return Err(conflict());
    }
    verify_open_file_state(temporary.as_file(), replacement_state)?;
    verify_path_references_open_file(temporary.path(), temporary.as_file())?;
    if expected == DestinationState::Missing {
        let committed = temporary
            .persist_noclobber(resolved_target)
            .map_err(|error| {
                if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                    conflict()
                } else {
                    QuickRowsError::from(error.error)
                }
            })?;
        let identity_matches =
            verify_path_references_open_file(resolved_target, &committed).is_ok();
        let replacement_matches = destination_state(resolved_target)
            == Ok(DestinationState::Existing(replacement_fingerprint));
        if identity_matches && replacement_matches && route_matches() {
            return Ok(());
        }
        return Err(QuickRowsError::destination_changed(format!(
            "{}; the unverified output was preserved at {}",
            conflict(),
            resolved_target.display()
        )));
    }

    ensure_destination_unchanged(resolved_target, expected)?;
    if !route_matches() {
        return Err(conflict());
    }
    verify_open_file_state(temporary.as_file(), replacement_state)?;
    verify_path_references_open_file(temporary.path(), temporary.as_file())?;

    #[cfg(windows)]
    return commit_temporary_windows(
        temporary,
        logical_target,
        resolved_target,
        expected,
        replacement_fingerprint,
    );

    #[cfg(not(windows))]
    match exchange_paths(temporary.path(), resolved_target) {
        Ok(()) => {
            let displaced_matches = destination_state(temporary.path()) == Ok(expected);
            let replacement_matches = destination_state(resolved_target)
                == Ok(DestinationState::Existing(replacement_fingerprint));
            if displaced_matches && replacement_matches && route_matches() {
                // `temporary.path()` now names the old destination. Dropping it
                // removes that displaced file while the new target stays live.
                drop(temporary);
                return Ok(());
            }
            if destination_state(resolved_target)
                != Ok(DestinationState::Existing(replacement_fingerprint))
            {
                let recovery = temporary
                    .into_temp_path()
                    .keep()
                    .map_err(|error| QuickRowsError::from(error.error))?;
                return Err(QuickRowsError::destination_changed(format!(
                    "CSV changed during save; the displaced destination is at {}",
                    recovery.display()
                )));
            }
            if let Err(rollback_error) = exchange_paths(temporary.path(), resolved_target) {
                let recovery = temporary
                    .into_temp_path()
                    .keep()
                    .map_err(|error| QuickRowsError::from(error.error))?;
                return Err(QuickRowsError::destination_changed(format!(
                    "CSV changed during save and rollback failed ({rollback_error}); the displaced destination is at {}",
                    recovery.display()
                )));
            }
            // Never unlink the exchanged path after a conflict: a writer could
            // have replaced the target immediately before the rollback, in
            // which case its file now occupies this unpredictable temp path.
            let recovery = temporary
                .into_temp_path()
                .keep()
                .map_err(|error| QuickRowsError::from(error.error))?;
            Err(QuickRowsError::destination_changed(format!(
                "{}; the uncommitted file was preserved at {}",
                conflict(),
                recovery.display()
            )))
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => Err(QuickRowsError::io(
            format!("This filesystem does not support conflict-safe replacement ({error}); use Save As to write a new file"),
        )),
        Err(error) => Err(QuickRowsError::from(error)),
    }
}

fn destination_state(path: &Path) -> QuickRowsResult<DestinationState> {
    match path.try_exists().map_err(QuickRowsError::from)? {
        true => file_fingerprint_cancellable(path, &|| false)
            .map(DestinationState::Existing)
            .map_err(|_| {
                QuickRowsError::destination_changed(
                    "CSV destination changed while preparing to save",
                )
            }),
        false => Ok(DestinationState::Missing),
    }
}

fn ensure_destination_unchanged(path: &Path, expected: DestinationState) -> QuickRowsResult<()> {
    let conflict = || {
        QuickRowsError::destination_changed(
            "CSV changed on disk; save was cancelled to protect the external changes",
        )
    };
    match destination_state(path) {
        Ok(current) if current == expected => Ok(()),
        Ok(_) | Err(_) => Err(conflict()),
    }
}

fn copy_destination_permissions(target: &Path, temporary: &Path) -> QuickRowsResult<()> {
    match std::fs::metadata(target) {
        Ok(metadata) => std::fs::set_permissions(temporary, metadata.permissions())
            .map_err(QuickRowsError::from),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(QuickRowsError::from(error)),
    }
}

fn cached_offsets_are_valid(offsets: &[u64], file_len: u64) -> bool {
    offsets.iter().all(|offset| *offset < file_len)
        && offsets.windows(2).all(|pair| pair[0] < pair[1])
}

fn cached_order_is_valid(order: &[usize], row_count: usize) -> bool {
    if order.len() != row_count {
        return false;
    }
    let mut seen = vec![false; row_count];
    for &row in order {
        let Some(slot) = seen.get_mut(row) else {
            return false;
        };
        if *slot {
            return false;
        }
        *slot = true;
    }
    true
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> QuickRowsResult<()> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(QuickRowsError::from)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> QuickRowsResult<()> {
    Ok(())
}
