//! Application settings model, path encoding, and durable persistence.

use crate::csv::ParseOverrides;
use crate::error::{QuickRowsError, QuickRowsResult};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const SETTINGS_VERSION: u32 = 1;
const MAX_RECENT_FILES: usize = 6;

mod model;
mod path_serde;
mod store;

pub use model::{AppSettings, RowDensity, ThemePreference};
pub use store::SettingsStore;

#[cfg(test)]
mod tests;
