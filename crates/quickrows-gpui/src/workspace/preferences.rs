// Display, parse, and theme preference behavior.
impl QuickRowsView {
    fn toggle_index(&mut self, _: &ToggleIndex, _window: &mut Window, cx: &mut Context<Self>) {
        self.preferences.settings.show_index = !self.preferences.settings.show_index;
        self.persist_settings();
        cx.notify();
    }

    fn compact_rows(&mut self, _: &CompactRows, _window: &mut Window, cx: &mut Context<Self>) {
        self.set_density(RowDensity::Compact, cx);
    }

    fn default_rows(&mut self, _: &DefaultRows, _window: &mut Window, cx: &mut Context<Self>) {
        self.set_density(RowDensity::Default, cx);
    }

    fn spacious_rows(&mut self, _: &SpaciousRows, _window: &mut Window, cx: &mut Context<Self>) {
        self.set_density(RowDensity::Spacious, cx);
    }

    fn set_density(&mut self, density: RowDensity, cx: &mut Context<Self>) {
        self.preferences.settings.row_density = density;
        self.persist_settings();
        cx.notify();
    }

    fn toggle_theme(&mut self, _: &ToggleTheme, window: &mut Window, cx: &mut Context<Self>) {
        let (preference, mode) = if cx.theme().mode.is_dark() {
            (ThemePreference::Light, ThemeMode::Light)
        } else {
            (ThemePreference::Dark, ThemeMode::Dark)
        };
        self.preferences.settings.theme = preference;
        Theme::change(mode, Some(window), cx);
        self.persist_settings();
        cx.notify();
    }

    fn open_settings(&mut self, _: &OpenSettings, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        self.overlay.modal = Modal::Settings;
        cx.notify();
    }

    fn open_parse_settings(
        &mut self,
        _: &OpenParseSettings,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_settings(&OpenSettings, _window, cx);
    }

    fn show_shortcuts(&mut self, _: &ShowShortcuts, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.overlay.modal = Modal::Shortcuts;
            cx.notify();
        }
    }

    fn show_about(&mut self, _: &ShowAbout, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.overlay.modal = Modal::About;
            cx.notify();
        }
    }

    fn close_settings(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        cx.notify();
    }

    fn close_info_modal(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        cx.notify();
    }

    fn apply_settings_choice(
        &mut self,
        choice: SettingsChoice,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let updates_parse_settings = !matches!(
            &choice,
            SettingsChoice::Theme(_) | SettingsChoice::Density(_)
        );
        let previous_parse_settings = self.preferences.settings.parse_overrides.clone();
        match choice {
            SettingsChoice::Theme(theme) => {
                self.set_theme_preference(theme, window, cx);
                return;
            }
            SettingsChoice::Density(density) => self.preferences.settings.row_density = density,
            SettingsChoice::Delimiter(value) => {
                self.preferences.settings.parse_overrides.delimiter = value.map(str::to_string)
            }
            SettingsChoice::Quote(value) => {
                self.preferences.settings.parse_overrides.quote = value.map(str::to_string)
            }
            SettingsChoice::Escape(value) => {
                self.preferences.settings.parse_overrides.escape = value.map(str::to_string)
            }
            SettingsChoice::Comment(value) => {
                self.preferences.settings.parse_overrides.comment = value.map(str::to_string)
            }
            SettingsChoice::ExcelSep(value) => self.preferences.settings.parse_overrides.excel_sep = value,
            SettingsChoice::LineEnding(value) => {
                self.preferences.settings.parse_overrides.line_ending = value.map(str::to_string)
            }
            SettingsChoice::Encoding(value) => {
                self.preferences.settings.parse_overrides.encoding = value.map(str::to_string)
            }
            SettingsChoice::Headers(value) => self.preferences.settings.parse_overrides.has_headers = value,
            SettingsChoice::Malformed(value) => {
                self.preferences.settings.parse_overrides.malformed = value.map(str::to_string)
            }
            SettingsChoice::MaxFieldSize(value) => {
                self.preferences.settings.parse_overrides.max_field_size = value
            }
            SettingsChoice::MaxRecordSize(value) => {
                self.preferences.settings.parse_overrides.max_record_size = value
            }
        }
        if updates_parse_settings
            && let Err(error) = validate_parse_overrides_for_info(
                &self.preferences.settings.parse_overrides,
                self.document.loaded.as_ref().map(|loaded| &loaded.parse_info),
            )
        {
            self.preferences.settings.parse_overrides = previous_parse_settings;
            self.feedback.error = Some(error.to_string().into());
            cx.notify();
            return;
        }
        self.feedback.error = None;
        self.persist_settings();
        cx.notify();
    }

    fn apply_custom_delimiter(&mut self, cx: &mut Context<Self>) {
        let value = self.inputs.custom_delimiter_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "delimiter",
            value,
            |overrides, value| overrides.delimiter = Some(value),
            cx,
        );
    }

    fn apply_custom_quote(&mut self, cx: &mut Context<Self>) {
        let value = self.inputs.custom_quote_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "quote",
            value,
            |overrides, value| overrides.quote = Some(value),
            cx,
        );
    }

    fn apply_custom_escape(&mut self, cx: &mut Context<Self>) {
        let value = self.inputs.custom_escape_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "escape",
            value,
            |overrides, value| overrides.escape = Some(value),
            cx,
        );
    }

    fn apply_custom_comment(&mut self, cx: &mut Context<Self>) {
        let value = self.inputs.custom_comment_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "comment",
            value,
            |overrides, value| overrides.comment = Some(value),
            cx,
        );
    }

    fn apply_custom_syntax_character(
        &mut self,
        name: &str,
        value: String,
        apply: impl FnOnce(&mut ParseOverrides, String),
        cx: &mut Context<Self>,
    ) {
        if !is_valid_syntax_character(&value) {
            self.feedback.error = Some(
                format!(
                    "Custom {name} must be exactly one character and cannot be NUL, CR, or LF."
                )
                .into(),
            );
            cx.notify();
            return;
        }
        let mut candidate = self.preferences.settings.parse_overrides.clone();
        apply(&mut candidate, value);
        if let Err(error) = validate_parse_overrides_for_info(
            &candidate,
            self.document.loaded.as_ref().map(|loaded| &loaded.parse_info),
        ) {
            self.feedback.error = Some(error.to_string().into());
            cx.notify();
            return;
        }
        self.preferences.settings.parse_overrides = candidate;
        self.feedback.error = None;
        self.persist_settings();
        cx.notify();
    }

    fn resolve_header_prompt(&mut self, use_first_row: Option<bool>, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        if let Some(use_first_row) = use_first_row {
            self.preferences.settings.parse_overrides.has_headers = Some(use_first_row);
            self.persist_settings();
            if let Some(path) = self.document.loaded.as_ref().map(|loaded| loaded.path.clone()) {
                self.reload_path(path, cx);
                return;
            }
        }
        cx.notify();
    }

    fn reload_with_parse_settings(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        let Some(path) = self.document.loaded.as_ref().map(|loaded| loaded.path.clone()) else {
            cx.notify();
            return;
        };
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Reload);
            cx.notify();
            return;
        }
        self.reload_path(path, cx);
    }

    fn reset_parse_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.preferences.settings.parse_overrides = Default::default();
        for input in [
            self.inputs.custom_delimiter_input.clone(),
            self.inputs.custom_quote_input.clone(),
            self.inputs.custom_escape_input.clone(),
            self.inputs.custom_comment_input.clone(),
        ] {
            input.update(cx, |input, cx| input.set_value("", window, cx));
        }
        self.feedback.error = None;
        self.persist_settings();
        cx.notify();
    }

    fn toggle_search_indexing(&mut self, cx: &mut Context<Self>) {
        self.preferences.settings.enable_indexing = !self.preferences.settings.enable_indexing;
        let enabled = self.preferences.settings.enable_indexing;
        self.persist_settings();
        let Some(document) = self.document.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            cx.notify();
            return;
        };
        if enabled {
            self.feedback.notice = Some(
                "Search indexing enabled; the selected column will be indexed on demand.".into(),
            );
            cx.notify();
        } else {
            match document.try_lock() {
                Ok(mut document) => {
                    document.clear_search_index();
                    self.feedback.notice = Some("Search index removed.".into());
                }
                Err(_) => {
                    self.feedback.notice = Some(
                        "Search indexing is disabled; the current operation is finishing.".into(),
                    );
                }
            }
            cx.notify();
        }
    }

    fn set_theme_preference(
        &mut self,
        preference: ThemePreference,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.preferences.settings.theme = preference;
        let mode = match preference {
            ThemePreference::Light => ThemeMode::Light,
            ThemePreference::Dark => ThemeMode::Dark,
            ThemePreference::System => ThemeMode::from(window.appearance()),
        };
        Theme::change(mode, Some(window), cx);
        self.persist_settings();
        cx.notify();
    }
}
