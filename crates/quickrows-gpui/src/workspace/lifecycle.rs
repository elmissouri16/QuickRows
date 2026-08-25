// Workspace initialization, settings persistence, and coarse selection state.
impl QuickRowsView {
    fn new(initial_path: Option<OpenTarget>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let settings_store = SettingsStore::new(settings_path());
        let (settings, settings_load_error) = load_settings_for_window(&settings_store);
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Find in CSV"));
        let custom_delimiter_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom delimiter"));
        if let Some(delimiter) = settings
            .parse_overrides
            .delimiter
            .as_deref()
            .filter(|delimiter| !is_named_delimiter(delimiter))
        {
            custom_delimiter_input.update(cx, |input, cx| {
                input.set_value(delimiter.to_string(), window, cx)
            });
        }
        let custom_quote_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom quote"));
        if let Some(quote) = settings
            .parse_overrides
            .quote
            .as_deref()
            .filter(|quote| !is_named_quote(quote))
        {
            custom_quote_input.update(cx, |input, cx| {
                input.set_value(quote.to_string(), window, cx)
            });
        }
        let custom_escape_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom escape"));
        if let Some(escape) = settings
            .parse_overrides
            .escape
            .as_deref()
            .filter(|escape| !is_named_escape(escape))
        {
            custom_escape_input.update(cx, |input, cx| {
                input.set_value(escape.to_string(), window, cx)
            });
        }
        let custom_comment_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom comment"));
        if let Some(comment) = settings
            .parse_overrides
            .comment
            .as_deref()
            .filter(|comment| !is_named_comment(comment))
        {
            custom_comment_input.update(cx, |input, cx| {
                input.set_value(comment.to_string(), window, cx)
            });
        }
        let edit_input = cx.new(|cx| InputState::new(window, cx).placeholder("Cell value"));
        cx.subscribe(&edit_input, |view, _, event, cx| match event {
            InputEvent::Change => {
                if let Some(initial_value) = view
                    .editor
                    .editing_cell
                    .as_ref()
                    .map(|editing| editing.initial_value.clone())
                {
                    view.editor.editing_draft_dirty = view.inputs.edit_input.read(cx).value() != initial_value;
                    cx.notify();
                }
            }
            InputEvent::PressEnter { secondary: false } | InputEvent::Blur => {
                view.commit_cell_edit(cx);
            }
            _ => {}
        })
        .detach();
        cx.subscribe(&search_input, |view, _, event, cx| match event {
            InputEvent::Change => view.schedule_search(cx),
            InputEvent::PressEnter { secondary } => {
                let query = view.inputs.search_input.read(cx).value().to_string();
                if view.queries.search.last_query.as_deref() != Some(query.as_str()) || view.queries.search.stale {
                    view.run_search(cx);
                } else if *secondary {
                    view.previous_search_result(cx);
                } else {
                    view.next_search_result(cx);
                }
            }
            _ => {}
        })
        .detach();
        let mut view = Self {
            focus_handle,
            document: DocumentState {
                pending_initial_path: initial_path,
                ..DocumentState::default()
            },
            operation: ForegroundOperation::default(),
            feedback: FeedbackState {
                error: settings_load_error,
                notice: None,
            },
            selection: WorkspaceSelection::default(),
            queries: QueryState::default(),
            inputs: InputEntities {
                search_input,
                custom_delimiter_input,
                custom_quote_input,
                custom_escape_input,
                custom_comment_input,
                edit_input,
            },
            editor: EditorState::default(),
            table: TableState::default(),
            overlay: OverlayState::default(),
            preferences: PreferencesState {
                settings,
                settings_store,
                show_warning_details: false,
            },
            runtime: RuntimeState::default(),
        };
        if let Some(path) = view.document.pending_initial_path.take() {
            view.open_path(path, cx);
        }
        view.focus_handle.focus(window);
        view
    }

    fn persist_settings(&mut self) {
        if let Err(error) = self.preferences.settings_store.save(&self.preferences.settings) {
            self.feedback.error = Some(format!("Unable to save settings: {error}").into());
        }
    }

    fn column_width(&self, column: usize) -> f32 {
        self.preferences.settings
            .column_widths
            .get(column)
            .copied()
            .unwrap_or(self.preferences.settings.column_width)
            .max(MIN_COLUMN_WIDTH)
    }

    fn set_column_width(&mut self, column: usize, width: f32) {
        let default_width = self.preferences.settings.column_width.max(MIN_COLUMN_WIDTH);
        if self.preferences.settings.column_widths.len() <= column {
            self.preferences.settings
                .column_widths
                .resize(column + 1, default_width);
        }
        self.preferences.settings.column_widths[column] = width.max(MIN_COLUMN_WIDTH);
        self.table.column_layout = None;
    }

    fn begin_column_resize(&mut self, column: usize, start_x: f32, cx: &mut Context<Self>) {
        self.table.resizing_column = Some(ColumnResize {
            column,
            start_x,
            start_width: self.column_width(column),
        });
        cx.notify();
    }

    fn update_column_resize(
        &mut self,
        event: &MouseMoveEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(resize) = self.table.resizing_column.as_ref() else {
            return;
        };
        let column = resize.column;
        let width = resize.start_width + f32::from(event.position.x) - resize.start_x;
        self.set_column_width(column, width);
        cx.notify();
    }

    fn finish_pointer_interaction(
        &mut self,
        _: &MouseUpEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let resized = self.table.resizing_column.take().is_some();
        let dragged_cells = std::mem::take(&mut self.selection.cell_dragging);
        if resized {
            self.persist_settings();
        }
        if resized || dragged_cells {
            cx.notify();
        }
    }

    fn clear_cell_editor(&mut self) {
        self.editor.editing_cell = None;
        self.editor.editing_draft_dirty = false;
        self.table.context_cell = None;
        self.editor.pending_cell_commits = self
            .editor.pending_cell_commits
            .saturating_sub(self.editor.cell_commit_queue.len());
        self.editor.cell_commit_queue.clear();
        self.editor.pending_edit_action = None;
    }

    fn clear_selection(&mut self) {
        self.selection.selected_row = None;
        self.selection.selected_rows.clear();
        self.selection.selection_anchor = None;
        self.selection.cell_selection = None;
        self.selection.cell_dragging = false;
    }

    fn apply_fragment_regions(&mut self, regions: &[ResolvedFragmentRegion]) {
        let Some(loaded) = self.document.loaded.as_ref() else {
            return;
        };
        let (rows, cells) = fragment_regions_to_selection(
            regions,
            loaded.row_count,
            loaded.headers.len(),
            loaded.parse_info.has_headers,
        );
        let first_row = rows.first().map(|rows| *rows.start());
        for rows in rows {
            self.selection.selected_rows.insert_range(rows);
        }
        if let Some(row) = first_row {
            self.selection.selected_row = Some(row);
            self.selection.selection_anchor = Some(row);
            self.table.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        }
        if let Some((start_row, start_column, end_row, end_column)) = cells {
            let mut selection = CellSelection::single(start_row, start_column);
            selection.set_active(end_row, end_column);
            self.selection.cell_selection = Some(selection);
        }
    }
}
