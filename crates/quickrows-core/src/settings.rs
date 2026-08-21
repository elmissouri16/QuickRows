use crate::csv::ParseOverrides;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const SETTINGS_VERSION: u32 = 1;
const MAX_RECENT_FILES: usize = 6;

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum StoredPath {
    Utf8(String),
    Encoded { encoding: String, data: String },
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    fn digit(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            b'A'..=b'F' => Some(value - b'A' + 10),
            _ => None,
        }
    }
    let chunks = value.as_bytes().chunks_exact(2);
    if !chunks.remainder().is_empty() {
        return None;
    }
    chunks
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

fn stored_path(path: &Path) -> StoredPath {
    if let Some(path) = path.to_str() {
        return StoredPath::Utf8(path.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        return StoredPath::Encoded {
            encoding: "unix-bytes".to_string(),
            data: encode_hex(path.as_os_str().as_bytes()),
        };
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let bytes = path
            .as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        return StoredPath::Encoded {
            encoding: "windows-wide".to_string(),
            data: encode_hex(&bytes),
        };
    }
    #[allow(unreachable_code)]
    StoredPath::Utf8(path.to_string_lossy().into_owned())
}

fn restore_path(path: StoredPath) -> Result<PathBuf, String> {
    match path {
        StoredPath::Utf8(path) => Ok(PathBuf::from(path)),
        StoredPath::Encoded { encoding, data } => {
            let bytes =
                decode_hex(&data).ok_or_else(|| "Invalid encoded settings path".to_string())?;
            #[cfg(unix)]
            if encoding == "unix-bytes" {
                use std::os::unix::ffi::OsStringExt;
                return Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)));
            }
            #[cfg(windows)]
            if encoding == "windows-wide" {
                use std::os::windows::ffi::OsStringExt;
                let chunks = bytes.chunks_exact(2);
                if !chunks.remainder().is_empty() {
                    return Err("Invalid Windows settings path".to_string());
                }
                let wide = chunks
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                    .collect::<Vec<_>>();
                return Ok(PathBuf::from(std::ffi::OsString::from_wide(&wide)));
            }
            Err(format!("Unsupported settings path encoding: {encoding}"))
        }
    }
}

mod optional_path {
    use super::{restore_path, stored_path, PathBuf, StoredPath};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(path: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        path.as_deref().map(stored_path).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<StoredPath>::deserialize(deserializer)?
            .map(restore_path)
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

mod path_vec {
    use super::{restore_path, stored_path, PathBuf, StoredPath};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(paths: &[PathBuf], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        paths
            .iter()
            .map(|path| stored_path(path))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<StoredPath>::deserialize(deserializer)?
            .into_iter()
            .map(restore_path)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ThemePreference {
    Light,
    Dark,
    #[default]
    System,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RowDensity {
    Compact,
    #[default]
    Default,
    Spacious,
}

impl RowDensity {
    pub fn height(self) -> f32 {
        match self {
            Self::Compact => 28.0,
            Self::Default => 36.0,
            Self::Spacious => 44.0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AppSettings {
    pub version: u32,
    pub show_index: bool,
    pub row_density: RowDensity,
    pub column_width: f32,
    pub column_widths: Vec<f32>,
    pub theme: ThemePreference,
    #[serde(with = "optional_path")]
    pub last_open_dir: Option<PathBuf>,
    #[serde(with = "path_vec")]
    pub recent_files: Vec<PathBuf>,
    pub enable_indexing: bool,
    pub parse_overrides: ParseOverrides,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            version: SETTINGS_VERSION,
            show_index: false,
            row_density: RowDensity::Default,
            column_width: 160.0,
            column_widths: Vec::new(),
            theme: ThemePreference::System,
            last_open_dir: None,
            recent_files: Vec::new(),
            // Full-value indexes trade substantial memory and another complete
            // file scan for faster repeated column searches. Keep this opt-in
            // so large files become viewable after the offset pass.
            enable_indexing: false,
            parse_overrides: ParseOverrides::default(),
        }
    }
}

impl AppSettings {
    pub fn normalize(&mut self) {
        self.version = SETTINGS_VERSION;
        self.column_width = self.column_width.max(120.0);
        for width in &mut self.column_widths {
            *width = width.max(120.0);
        }
        let mut unique = Vec::with_capacity(self.recent_files.len().min(MAX_RECENT_FILES));
        for path in self.recent_files.drain(..) {
            if !unique.contains(&path) {
                unique.push(path);
            }
            if unique.len() == MAX_RECENT_FILES {
                break;
            }
        }
        self.recent_files = unique;
    }

    pub fn remember_file(&mut self, path: PathBuf) {
        self.recent_files.retain(|recent| recent != &path);
        self.recent_files.insert(0, path.clone());
        self.recent_files.truncate(MAX_RECENT_FILES);
        self.last_open_dir = path.parent().map(Path::to_path_buf);
    }
}

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

    pub fn load(&self) -> Result<AppSettings, String> {
        let contents = match fs::read_to_string(&self.path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AppSettings::default())
            }
            Err(err) => return Err(err.to_string()),
        };
        let mut settings: AppSettings =
            serde_json::from_str(&contents).map_err(|e| e.to_string())?;
        settings.normalize();
        Ok(settings)
    }

    pub fn save(&self, settings: &AppSettings) -> Result<(), String> {
        let mut normalized = settings.clone();
        normalized.normalize();
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let bytes = serde_json::to_vec_pretty(&normalized).map_err(|e| e.to_string())?;
        let mut temporary = tempfile::Builder::new()
            .prefix(".quickrows-settings-")
            .suffix(".tmp")
            .tempfile_in(parent)
            .map_err(|e| e.to_string())?;
        temporary.write_all(&bytes).map_err(|e| e.to_string())?;
        temporary.as_file().sync_all().map_err(|e| e.to_string())?;
        temporary
            .persist(&self.path)
            .map_err(|error| error.error.to_string())?;
        sync_directory(parent)
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), String> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| e.to_string())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip_and_normalize() {
        let dir = tempfile::tempdir().unwrap();
        let store = SettingsStore::new(dir.path().join("settings.json"));
        let mut settings = AppSettings::default();
        settings.column_width = 20.0;
        settings.remember_file(PathBuf::from("/tmp/example.csv"));
        store.save(&settings).unwrap();
        settings.column_width = 180.0;
        store.save(&settings).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.column_width, 180.0);
        assert_eq!(loaded.recent_files, vec![PathBuf::from("/tmp/example.csv")]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_recent_paths_round_trip() {
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::tempdir().unwrap();
        let store = SettingsStore::new(dir.path().join("settings.json"));
        let mut bytes = b"/tmp/rows-".to_vec();
        bytes.push(0xff);
        bytes.extend_from_slice(b".csv");
        let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
        let mut settings = AppSettings::default();
        settings.remember_file(path.clone());

        store.save(&settings).unwrap();
        let loaded = store.load().unwrap();

        assert_eq!(loaded.recent_files, vec![path.clone()]);
        assert_eq!(loaded.last_open_dir, path.parent().map(Path::to_path_buf));
    }

    #[test]
    fn parse_overrides_round_trip_with_extended_dialects() {
        let dir = tempfile::tempdir().unwrap();
        let store = SettingsStore::new(dir.path().join("settings.json"));
        let mut settings = AppSettings::default();
        settings.parse_overrides = ParseOverrides {
            delimiter: Some("§".to_string()),
            quote: Some("«".to_string()),
            escape: Some("※".to_string()),
            comment: Some("#".to_string()),
            excel_sep: Some(false),
            line_ending: Some("crlf".to_string()),
            encoding: Some("utf-16le".to_string()),
            has_headers: Some(false),
            malformed: Some("strict".to_string()),
            max_field_size: Some(1024),
            max_record_size: Some(4096),
        };
        store.save(&settings).unwrap();

        let loaded = store.load().unwrap();
        let parse = loaded.parse_overrides;
        assert_eq!(parse.delimiter.as_deref(), Some("§"));
        assert_eq!(parse.quote.as_deref(), Some("«"));
        assert_eq!(parse.escape.as_deref(), Some("※"));
        assert_eq!(parse.comment.as_deref(), Some("#"));
        assert_eq!(parse.excel_sep, Some(false));
        assert_eq!(parse.encoding.as_deref(), Some("utf-16le"));
        assert_eq!(parse.max_record_size, Some(4096));
    }

    #[test]
    fn older_settings_without_extended_parse_fields_still_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(
            &path,
            r#"{
                "version": 1,
                "parse_overrides": {
                    "delimiter": "semicolon",
                    "has_headers": true
                }
            }"#,
        )
        .unwrap();
        let loaded = SettingsStore::new(path).load().unwrap();
        assert_eq!(
            loaded.parse_overrides.delimiter.as_deref(),
            Some("semicolon")
        );
        assert_eq!(loaded.parse_overrides.comment, None);
        assert_eq!(loaded.parse_overrides.excel_sep, None);
    }

    #[test]
    fn missing_settings_use_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let store = SettingsStore::new(dir.path().join("missing.json"));
        assert_eq!(store.load().unwrap().row_density, RowDensity::Default);
    }
}
