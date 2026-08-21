use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

const DEBUG_LOG_FILE: &str = "quickrows.log";
const CRASH_LOG_FILE: &str = "quickrows-crash.log";
const LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Diagnostics {
    directory: PathBuf,
    enabled: Arc<AtomicBool>,
}

impl Diagnostics {
    pub fn disabled(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new(directory: impl Into<PathBuf>, enabled: bool) -> Result<Self, String> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        Ok(Self {
            directory,
            enabled: Arc::new(AtomicBool::new(enabled)),
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn debug_log_path(&self) -> PathBuf {
        self.directory.join(DEBUG_LOG_FILE)
    }

    pub fn crash_log_path(&self) -> PathBuf {
        self.directory.join(CRASH_LOG_FILE)
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) -> Result<(), String> {
        self.enabled.store(enabled, Ordering::Relaxed);
        if enabled {
            self.append_debug("debug logging enabled")?;
        }
        Ok(())
    }

    pub fn append_debug(&self, message: &str) -> Result<(), String> {
        if !self.enabled() {
            return Ok(());
        }
        append_line(&self.debug_log_path(), message)
    }

    pub fn clear_debug_log(&self) -> Result<(), String> {
        fs::write(self.debug_log_path(), b"").map_err(|e| e.to_string())
    }

    pub fn install_panic_hook(&self) {
        let diagnostics = self.clone();
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let backtrace = std::backtrace::Backtrace::force_capture();
            let message = format!("PANIC: {info}\n{backtrace}");
            let _ = append_line(&diagnostics.crash_log_path(), &message);
            if diagnostics.enabled() {
                let _ = append_line(&diagnostics.debug_log_path(), &message);
            }
            previous(info);
        }));
    }
}

fn append_line(path: &Path, message: &str) -> Result<(), String> {
    if fs::metadata(path).map(|meta| meta.len()).unwrap_or(0) > LOG_MAX_BYTES {
        fs::write(path, b"").map_err(|e| e.to_string())?;
    }
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let timestamp = format!("{}.{:03}", duration.as_secs(), duration.subsec_millis());
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    writeln!(file, "[{timestamp}] {}", truncate_utf8(message, 16 * 1024)).map_err(|e| e.to_string())
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_log_respects_toggle_and_utf8() {
        let directory = tempfile::tempdir().unwrap();
        let diagnostics = Diagnostics::new(directory.path(), false).unwrap();
        diagnostics.append_debug("hidden").unwrap();
        assert!(!diagnostics.debug_log_path().exists());
        diagnostics.set_enabled(true).unwrap();
        diagnostics.append_debug(&"é".repeat(20_000)).unwrap();
        let contents = fs::read_to_string(diagnostics.debug_log_path()).unwrap();
        assert!(contents.contains("debug logging enabled"));
        assert!(contents.is_char_boundary(contents.len()));
        diagnostics.clear_debug_log().unwrap();
        assert_eq!(
            fs::read_to_string(diagnostics.debug_log_path()).unwrap(),
            ""
        );
    }
}
