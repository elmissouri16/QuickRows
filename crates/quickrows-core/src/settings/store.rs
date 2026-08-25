use super::model::AppSettings;
use super::*;
use std::fs;
use std::io::Write;

#[derive(Clone, Debug)]
pub struct SettingsStore {
    path: PathBuf,
}

impl SettingsStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> QuickRowsResult<AppSettings> {
        let contents = match fs::read_to_string(&self.path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AppSettings::default());
            }
            Err(err) => return Err(QuickRowsError::from(err)),
        };
        let mut settings: AppSettings = serde_json::from_str(&contents)
            .map_err(|error| QuickRowsError::invalid_settings(error.to_string()))?;
        settings.normalize();
        Ok(settings)
    }

    pub fn save(&self, settings: &AppSettings) -> QuickRowsResult<()> {
        let mut normalized = settings.clone();
        normalized.normalize();
        let bytes = serde_json::to_vec_pretty(&normalized)
            .map_err(|error| QuickRowsError::invalid_settings(error.to_string()))?;
        crate::storage::write_file_atomically(
            &self.path,
            ".quickrows-settings-",
            ".tmp",
            8 * 1024,
            crate::storage::Durability::SyncFileAndDirectory,
            |writer| writer.write_all(&bytes).map_err(QuickRowsError::from),
        )
    }
}
