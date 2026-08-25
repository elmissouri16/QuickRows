use super::open_state::{FileChangeToken, FileIdentity, file_change_token, file_identity};
use super::*;

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
) -> QuickRowsResult<ObservedFingerprint> {
    let metadata = source.metadata().map_err(QuickRowsError::from)?;
    if !metadata.is_file() {
        return Err(QuickRowsError::invalid_csv(
            "CSV source is not a regular file",
        ));
    }
    let expected_len = metadata.len();
    let modified = metadata_modified(&metadata);
    let change_before = file_change_token(source)?;
    let (len, content_hash) = hash_snapshot(source, is_cancelled)?;
    check_snapshot_cancellation(is_cancelled)?;
    let metadata_after = source.metadata().map_err(QuickRowsError::from)?;
    let change_after = file_change_token(source)?;
    if len != expected_len
        || metadata_after.len() != expected_len
        || metadata_modified(&metadata_after) != modified
    {
        return Err(QuickRowsError::source_changed(
            "CSV changed on disk while it was being fingerprinted",
        ));
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

pub(super) fn file_fingerprint_with_identity(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
    identity_of: &dyn Fn(&std::fs::File) -> QuickRowsResult<Option<FileIdentity>>,
) -> QuickRowsResult<FileFingerprint> {
    check_snapshot_cancellation(is_cancelled)?;
    let mut source = std::fs::File::open(path).map_err(QuickRowsError::from)?;
    let identity = identity_of(&source)?;
    let observed = fingerprint_open_file(&mut source, is_cancelled)?;
    let fingerprint = observed.fingerprint;
    check_snapshot_cancellation(is_cancelled)?;

    // Reopen the path after hashing so a replacement or symlink retarget cannot
    // hide behind the still-live descriptor for the original referent.
    let mut current = std::fs::File::open(path).map_err(|_| {
        QuickRowsError::source_changed("CSV changed on disk while it was being fingerprinted")
    })?;
    let current_metadata = current.metadata().map_err(QuickRowsError::from)?;
    if current_metadata.len() != fingerprint.len
        || metadata_modified(&current_metadata) != fingerprint.modified
    {
        return Err(QuickRowsError::source_changed(
            "CSV changed on disk while it was being fingerprinted",
        ));
    }
    let current_identity = identity_of(&current)?;
    check_snapshot_cancellation(is_cancelled)?;
    let current_change = file_change_token(&current)?;
    let identity_is_stable = match (identity, current_identity) {
        (Some(expected), Some(actual)) if expected == actual => true,
        (Some(_), Some(_)) => {
            return Err(QuickRowsError::source_changed(
                "CSV changed on disk while it was being fingerprinted",
            ));
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
        return Err(QuickRowsError::source_changed(
            "CSV changed on disk while it was being fingerprinted",
        ));
    }

    // Validate the fallback handle against the path again so a retarget during
    // its hash cannot hide behind that descriptor either.
    let mut latest = std::fs::File::open(path).map_err(|_| {
        QuickRowsError::source_changed("CSV changed on disk while it was being fingerprinted")
    })?;
    let latest_metadata = latest.metadata().map_err(QuickRowsError::from)?;
    if latest_metadata.len() != fingerprint.len
        || metadata_modified(&latest_metadata) != fingerprint.modified
    {
        return Err(QuickRowsError::source_changed(
            "CSV changed on disk while it was being fingerprinted",
        ));
    }
    let latest_identity = identity_of(&latest)?;
    check_snapshot_cancellation(is_cancelled)?;
    let latest_change = file_change_token(&latest)?;
    match (current_identity, latest_identity) {
        (Some(expected), Some(actual)) if expected != actual => {
            return Err(QuickRowsError::source_changed(
                "CSV changed on disk while it was being fingerprinted",
            ));
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
        Err(QuickRowsError::source_changed(
            "CSV changed on disk while it was being fingerprinted",
        ))
    } else {
        Ok(fingerprint)
    }
}

pub(super) fn file_fingerprint_with_cancellation(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<FileFingerprint> {
    file_fingerprint_with_identity(path, is_cancelled, &file_identity)
}

pub(crate) fn file_fingerprint_cancellable(
    path: &Path,
    is_cancelled: &dyn Fn() -> bool,
) -> QuickRowsResult<FileFingerprint> {
    file_fingerprint_with_cancellation(path, is_cancelled)
}
