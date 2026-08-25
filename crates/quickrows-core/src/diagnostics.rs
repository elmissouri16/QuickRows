use crate::error::{QuickRowsError, QuickRowsResult};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

const DEBUG_LOG_FILE: &str = "quickrows.log";
const CRASH_LOG_FILE: &str = "quickrows-crash.log";
const LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;
static LOG_WRITE_LOCK: Mutex<()> = Mutex::new(());

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

    pub fn new(directory: impl Into<PathBuf>, enabled: bool) -> QuickRowsResult<Self> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(QuickRowsError::from)?;
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

    pub fn set_enabled(&self, enabled: bool) -> QuickRowsResult<()> {
        self.enabled.store(enabled, Ordering::Relaxed);
        if enabled {
            self.append_debug("debug logging enabled")?;
        }
        Ok(())
    }

    pub fn append_debug(&self, message: &str) -> QuickRowsResult<()> {
        if !self.enabled() {
            return Ok(());
        }
        append_line(&self.debug_log_path(), message)
    }

    pub fn clear_debug_log(&self) -> QuickRowsResult<()> {
        let _guard = LOG_WRITE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        fs::write(self.debug_log_path(), b"").map_err(QuickRowsError::from)
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

fn append_line(path: &Path, message: &str) -> QuickRowsResult<()> {
    let _guard = LOG_WRITE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if fs::metadata(path).map(|meta| meta.len()).unwrap_or(0) > LOG_MAX_BYTES {
        fs::write(path, b"").map_err(QuickRowsError::from)?;
    }
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let timestamp = format!("{}.{:03}", duration.as_secs(), duration.subsec_millis());
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(QuickRowsError::from)?;
    writeln!(file, "[{timestamp}] {}", truncate_utf8(message, 16 * 1024))
        .map_err(QuickRowsError::from)
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
    fn concurrent_debug_writes_do_not_lose_lines() {
        let directory = tempfile::tempdir().unwrap();
        let diagnostics = Diagnostics::new(directory.path(), true).unwrap();
        let workers = (0..8)
            .map(|worker| {
                let diagnostics = diagnostics.clone();
                std::thread::spawn(move || {
                    for line in 0..100 {
                        diagnostics
                            .append_debug(&format!("worker {worker} line {line}"))
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }

        let lines = fs::read_to_string(diagnostics.debug_log_path())
            .unwrap()
            .lines()
            .count();
        assert_eq!(lines, 800);
    }

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
