use super::path_serde::{optional_path, path_vec};
use super::*;

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
