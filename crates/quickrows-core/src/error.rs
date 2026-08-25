//! Typed failures exposed by the QuickRows engine.
//!
//! User-facing text remains available through [`Display`](std::fmt::Display),
//! while callers should branch on [`QuickRowsError::kind`] instead of parsing
//! that text.

use std::fmt;

/// Stable, machine-readable categories for engine failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Cancelled,
    SourceChanged,
    DestinationChanged,
    InvalidSettings,
    InvalidCsv,
    CacheCorrupt,
    OutOfRange,
    Io,
    Other,
}

/// An engine failure with a stable category and a user-facing message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuickRowsError {
    kind: ErrorKind,
    message: String,
}

/// Result type returned by typed QuickRows APIs.
pub type QuickRowsResult<T> = std::result::Result<T, QuickRowsError>;

impl QuickRowsError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "Operation cancelled")
    }

    pub fn source_changed(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::SourceChanged, message)
    }

    pub fn destination_changed(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::DestinationChanged, message)
    }

    pub fn invalid_settings(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidSettings, message)
    }

    pub fn invalid_csv(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidCsv, message)
    }

    pub fn cache_corrupt(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::CacheCorrupt, message)
    }

    pub fn out_of_range(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::OutOfRange, message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Io, message)
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other, message)
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Compatibility helper for callers being migrated from `String` errors.
    /// New control flow should use [`Self::kind`].
    pub fn contains(&self, pattern: &str) -> bool {
        self.message.contains(pattern)
    }

    pub(crate) fn from_legacy_message(message: String) -> Self {
        let lowercase = message.to_ascii_lowercase();
        let kind = if lowercase.contains("cancelled") {
            ErrorKind::Cancelled
        } else if lowercase.contains("out of range") || lowercase.contains("out of bounds") {
            ErrorKind::OutOfRange
        } else if lowercase.contains("destination changed")
            || lowercase.contains("saved csv changed")
        {
            ErrorKind::DestinationChanged
        } else if lowercase.contains("changed on disk") || lowercase.contains("source changed") {
            ErrorKind::SourceChanged
        } else if lowercase.contains("cache")
            && (lowercase.contains("invalid")
                || lowercase.contains("corrupt")
                || lowercase.contains("mismatch"))
        {
            ErrorKind::CacheCorrupt
        } else if lowercase.contains("parse override")
            || lowercase.contains("parse setting")
            || lowercase.contains("unsupported csv")
        {
            ErrorKind::InvalidSettings
        } else if lowercase.contains("csv")
            || lowercase.contains("encoding")
            || lowercase.contains("record")
            || lowercase.contains("field")
        {
            ErrorKind::InvalidCsv
        } else {
            ErrorKind::Other
        };
        Self { kind, message }
    }
}

impl fmt::Display for QuickRowsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QuickRowsError {}

impl From<std::io::Error> for QuickRowsError {
    fn from(error: std::io::Error) -> Self {
        Self::io(error.to_string())
    }
}

impl From<Box<dyn std::error::Error>> for QuickRowsError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        match error.downcast::<std::io::Error>() {
            Ok(error) => Self::from(*error),
            Err(error) => Self::invalid_csv(error.to_string()),
        }
    }
}

impl From<String> for QuickRowsError {
    fn from(message: String) -> Self {
        Self::from_legacy_message(message)
    }
}

impl From<QuickRowsError> for String {
    fn from(error: QuickRowsError) -> Self {
        error.message
    }
}

impl From<&str> for QuickRowsError {
    fn from(message: &str) -> Self {
        Self::from_legacy_message(message.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_messages_are_classified_at_the_compatibility_boundary() {
        let cases = [
            ("Operation cancelled", ErrorKind::Cancelled),
            (
                "CSV changed on disk while it was being opened",
                ErrorKind::SourceChanged,
            ),
            (
                "CSV destination changed while preparing to save",
                ErrorKind::DestinationChanged,
            ),
            ("CSV parse settings are invalid", ErrorKind::InvalidSettings),
            ("Invalid CSV record", ErrorKind::InvalidCsv),
            ("CSV cache header mismatch", ErrorKind::CacheCorrupt),
            ("Sort column is out of range", ErrorKind::OutOfRange),
        ];
        for (message, expected) in cases {
            assert_eq!(QuickRowsError::from(message).kind(), expected, "{message}");
        }
    }

    #[test]
    fn display_preserves_the_original_user_facing_message() {
        let error = QuickRowsError::source_changed("source changed during open");
        assert_eq!(error.to_string(), "source changed during open");
    }

    #[test]
    fn io_conversion_is_typed() {
        let error = QuickRowsError::from(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        assert_eq!(error.kind(), ErrorKind::Io);
        assert!(error.contains("permission denied"));
    }
}
