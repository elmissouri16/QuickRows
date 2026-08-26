mod actions;
mod cell_selection;
mod selection;
#[cfg(test)]
mod state_tests;

use actions::*;
use cell_selection::CellSelection;
use directories::ProjectDirs;
#[cfg(all(not(target_os = "macos"), not(test)))]
use gpui::PathPromptOptions;
use gpui::{
    App, Application, Axis, Bounds, ClickEvent, ClipboardItem, Context, FocusHandle, KeyBinding,
    KeyDownEvent, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent, Pixels, Point,
    ScrollHandle, ScrollStrategy, SharedString, StatefulInteractiveElement,
    UniformListScrollHandle, WeakEntity, Window, WindowBounds, WindowOptions, div, prelude::*, px,
    relative, size, uniform_list,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, IconName, Root, Theme, ThemeMode,
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Escape as InputEscape, Input, InputEvent, InputState, SelectAll as InputSelectAll},
    menu::{DropdownMenu as _, PopupMenuItem},
    scroll::{ScrollableElement as _, ScrollableMask, Scrollbar, ScrollbarShow},
    switch::Switch,
    v_flex,
};
use gpui_component_assets::Assets;
use quickrows_core::{
    AppSettings, CancellationToken, CsvDocument, CsvFragment, Diagnostics, ErrorKind,
    FileFingerprint, ParseInfo, ParseOverrides, ParseWarning, QuickRowsError,
    ResolvedFragmentRegion, RowDensity, SettingsStore, SortDirection, SortSpec, ThemePreference,
    validate_parse_overrides_for_info,
};
use selection::RowSelection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

const BASE_TITLE: &str = "QuickRows";
const MIN_COLUMN_WIDTH: f32 = 120.0;
const ROW_INDEX_WIDTH: f32 = 72.0;
const TABLE_SCROLLBAR_THICKNESS: f32 = 16.0;
const MIN_TABLE_WIDTH: f32 = 640.0;
const COLUMN_OVERSCAN_WIDTH: f32 = MIN_COLUMN_WIDTH * 2.0;
const COLUMN_RESIZE_HANDLE_WIDTH: f32 = 8.0;
const DELETE_CONFIRM_THRESHOLD: usize = 1_000;
const COPY_CONFIRM_THRESHOLD: usize = 5_000;
const MAX_CACHED_ROWS: usize = 1_024;
const INSTANCE_PORT: u16 = 47_391;
const INSTANCE_MAGIC: &str = "QUICKROWS-INSTANCE-1";

// These concern-oriented fragments intentionally share one private module namespace.
// That keeps QuickRowsView internals private while avoiding a monolithic source file.
include!("workspace/state.rs");
include!("workspace/lifecycle.rs");
include!("workspace/row_loading.rs");
include!("workspace/document_io.rs");
include!("workspace/queries.rs");
include!("workspace/mutations.rs");
include!("workspace/editing.rs");
include!("workspace/selection_input.rs");
include!("workspace/preferences.rs");
include!("workspace/clipboard.rs");
include!("workspace/overlays.rs");
include!("workspace/settings_view.rs");
include!("workspace/table_view.rs");
include!("workspace/context_menu.rs");
include!("workspace/render.rs");
include!("workspace/presentation.rs");
include!("workspace/platform.rs");
include!("workspace/app.rs");
