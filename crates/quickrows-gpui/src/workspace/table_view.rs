// Virtualized table and command-bar rendering.
impl QuickRowsView {
    fn render_table(&mut self, window: &mut Window, cx: &mut Context<Self>) -> gpui::AnyElement {
        let Some(loaded) = &self.document.loaded else {
            window.set_window_title(BASE_TITLE);
            return self.render_empty(cx);
        };
        let headers = loaded.headers.clone();
        let header_labels = loaded.header_labels.clone();
        let row_count = loaded.row_count;
        let filename = display_name(&loaded.path);
        let loaded_dirty = loaded.dirty;
        let viewport_width =
            (f32::from(window.viewport_size().width) - TABLE_SCROLLBAR_THICKNESS).max(0.0);
        let show_toolbar_labels = toolbar_shows_labels(f32::from(window.viewport_size().width));
        let show_index = self.preferences.settings.show_index;
        let row_height = self.preferences.settings.row_density.height();
        if self
            .table.column_layout
            .as_ref()
            .is_none_or(|layout| layout.widths.len() != headers.len())
        {
            self.table.column_layout = Some(ColumnLayout::from_settings(headers.len(), &self.preferences.settings));
        }
        let column_layout = self
            .table.column_layout
            .as_ref()
            .expect("column layout is initialized")
            .clone();
        let leading_width = if show_index { ROW_INDEX_WIDTH } else { 0.0 };
        let table_width =
            (leading_width + column_layout.total_width()).clamp(MIN_TABLE_WIDTH, f32::MAX);
        let scroll_left = (-f32::from(self.table.column_scroll.offset().x))
            .clamp(0.0, (table_width - viewport_width).max(0.0));
        let column_plan = column_render_plan(
            &column_layout,
            scroll_left,
            viewport_width,
            leading_width,
            [
                self.editor.editing_cell.as_ref().map(|editing| editing.column),
                self.table.resizing_column.as_ref().map(|resize| resize.column),
            ]
            .into_iter()
            .flatten(),
        );
        let selected_rows = self.selection.selected_rows.clone();
        let selected_count = selected_rows.len();
        let cell_selection = self.selection.cell_selection;
        let cell_dimensions = cell_selection.map(CellSelection::dimensions);
        let search_rows: HashSet<usize> =
            if self.queries.active_highlight == Some(ActiveHighlight::Search) && !self.queries.search.stale {
                self.table.row_cache
                    .values()
                    .filter_map(|row| {
                        self.queries.search.results
                            .binary_search(&row.source_row)
                            .is_ok()
                            .then_some(row.source_row)
                    })
                    .collect()
            } else {
                HashSet::new()
            };
        let duplicate_rows: HashSet<usize> = if self.queries.active_highlight
            == Some(ActiveHighlight::Duplicates)
            && !self.queries.duplicates.stale
        {
            self.table.row_cache
                .values()
                .filter_map(|row| {
                    self.queries.duplicates.results
                        .binary_search(&row.source_row)
                        .is_ok()
                        .then_some(row.source_row)
                })
                .collect()
        } else {
            HashSet::new()
        };
        let current_source_match = (self.queries.active_highlight == Some(ActiveHighlight::Search)
            && !self.queries.search.stale)
            .then(|| self.queries.search.results.get(self.queries.search.current_match).copied())
            .flatten();
        let current_duplicate_source = (self.queries.active_highlight == Some(ActiveHighlight::Duplicates)
            && !self.queries.duplicates.stale)
            .then(|| {
                self.queries.duplicates.results
                    .get(self.queries.duplicates.current_match)
                    .copied()
            })
            .flatten();
        let dirty = loaded_dirty || self.editor.pending_cell_commits > 0 || self.editor.editing_draft_dirty;
        let search_scope = column_scope_label(self.queries.search.column, &header_labels);
        let duplicate_scope = column_scope_label(self.queries.duplicates.column, &header_labels);
        let query_view = self.runtime.self_weak.clone();
        let cell_search = (self.queries.active_highlight == Some(ActiveHighlight::Search)
            && !self.queries.search.stale)
            .then(|| {
                self.queries.search.last_query.clone().map(|query| {
                    (
                        query,
                        self.queries.search.column,
                        self.queries.search.match_case,
                        self.queries.search.whole_word,
                    )
                })
            })
            .flatten();
        window.set_window_title(&format!(
            "{}{} - {}",
            filename,
            if dirty { " *" } else { "" },
            BASE_TITLE
        ));

        let header_columns = virtual_column_children(&column_layout, &column_plan, |index| {
            header_cell(header_labels[index].clone(), column_layout.width(index), cx)
                .id(("header", index))
                .relative()
                .cursor_pointer()
                .on_click(cx.listener(move |this, _, _, cx| this.sort_column(index, cx)))
                .child(
                    div()
                        .id(("column-resizer", index))
                        .absolute()
                        .right_0()
                        .top_0()
                        .bottom_0()
                        .w(px(COLUMN_RESIZE_HANDLE_WIDTH))
                        .cursor_col_resize()
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, event: &MouseDownEvent, _, cx| {
                                this.begin_column_resize(index, f32::from(event.position.x), cx);
                                cx.stop_propagation();
                            }),
                        )
                        .on_click(|_, _, cx| cx.stop_propagation()),
                )
                .into_any_element()
        });
        let header_content = h_flex()
            .w(px(table_width))
            .h(px(row_height))
            .flex_none()
            .border_b_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().secondary.opacity(0.65))
            .when(show_index, |this| {
                this.child(header_cell("#", ROW_INDEX_WIDTH, cx))
            })
            .children(header_columns);

        // GPUI Component's ScrollableMask and Scrollbar both notify the current
        // view whenever they update this shared handle. Each wheel or thumb
        // movement therefore rebuilds the virtual column plan before paint, and
        // the header and body consume that same plan.
        let header = div()
            .id("table-header-scroll")
            .w_full()
            .h(px(row_height))
            .flex_none()
            .overflow_scroll()
            .track_scroll(&self.table.column_scroll)
            .child(header_content);

        let rows = uniform_list(
            "csv-rows",
            row_count,
            cx.processor(move |this, range: std::ops::Range<usize>, _window, cx| {
                this.request_visible_rows(range.clone(), cx);
                range
                    .map(|display_row| {
                        let cached = this.table.row_cache.get(&display_row).cloned();
                        let source_row = cached.as_ref().map(|row| row.source_row);
                        let is_deleted = cached.as_ref().is_some_and(|row| row.deleted);
                        let can_edit = cached.is_some() && !is_deleted;
                        let editing_cell = this.editor.editing_cell.clone();
                        let edit_input = this.inputs.edit_input.clone();
                        let cells = cached.as_ref().map(|row| row.cells.clone());
                        let is_selected = selected_rows.contains(display_row);
                        let is_search_match =
                            source_row.is_some_and(|row| search_rows.contains(&row));
                        let is_current_match = source_row == current_source_match
                            || source_row == current_duplicate_source;
                        let is_duplicate =
                            source_row.is_some_and(|row| duplicate_rows.contains(&row));
                        h_flex()
                            .id(("row", display_row))
                            .h(px(row_height))
                            .w(px(table_width))
                            .flex_none()
                            .border_b_1()
                            .border_color(cx.theme().border)
                            .when(!is_selected && !is_current_match, |row| {
                                row.hover(|row| row.bg(cx.theme().table_hover))
                            })
                            .when(!is_selected && display_row % 2 == 1, |row| {
                                row.bg(cx.theme().table_even)
                            })
                            .when(is_duplicate, |row| row.bg(cx.theme().warning.opacity(0.18)))
                            .when(is_search_match, |row| row.bg(cx.theme().info.opacity(0.18)))
                            .when(is_deleted && !is_selected, |row| {
                                row.bg(cx.theme().danger.opacity(0.16)).opacity(0.65)
                            })
                            .when(is_current_match && !is_selected, |row| {
                                row.bg(cx.theme().warning.opacity(0.24))
                                    .border_1()
                                    .border_color(cx.theme().warning)
                            })
                            .when(is_selected, |row| {
                                row.bg(cx.theme().selection.opacity(0.72))
                                    .border_1()
                                    .border_color(cx.theme().foreground.opacity(0.55))
                            })
                            .on_click(cx.listener(move |this, event: &ClickEvent, _, cx| {
                                this.select_row_from_click(display_row, event, cx);
                            }))
                            .on_mouse_down(
                                MouseButton::Right,
                                cx.listener(move |this, _, _, cx| {
                                    if !this.selection.selected_rows.contains(display_row) {
                                        this.selection.selected_rows.select_only(display_row);
                                        this.selection.selection_anchor = Some(display_row);
                                    }
                                    this.selection.selected_row = Some(display_row);
                                    cx.notify();
                                }),
                            )
                            .when(show_index, |row| {
                                let index = source_row
                                    .map(|source_row| source_row + 1)
                                    .unwrap_or(display_row + 1);
                                row.child(
                                    body_cell(index.to_string(), ROW_INDEX_WIDTH, cx)
                                        .id(("row-index", display_row))
                                        .on_mouse_down(
                                            MouseButton::Right,
                                            cx.listener(
                                                move |this, event: &MouseDownEvent, _, cx| {
                                                    this.table.context_cell = None;
                                                    this.selection.cell_selection = None;
                                                    this.table.table_context_menu =
                                                        Some(TableContextMenu {
                                                            position: event.position,
                                                            kind: TableContextMenuKind::Row,
                                                            focused_item: 0,
                                                        });
                                                    cx.notify();
                                                },
                                            ),
                                        ),
                                )
                            })
                            .children(virtual_column_children(
                                &column_layout,
                                &column_plan,
                                |index| {
                                    let value = cells
                                        .as_ref()
                                        .and_then(|cells| cells.get(index))
                                        .cloned()
                                        .unwrap_or_else(|| {
                                            if cells.is_none() && index == 0 {
                                                SharedString::from("Loading…")
                                            } else {
                                                SharedString::from("")
                                            }
                                        });
                                    let width = column_layout.width(index);
                                    let is_cell_match = cell_search.as_ref().is_some_and(
                                        |(query, column, match_case, whole_word)| {
                                            column.is_none_or(|column| column == index)
                                                && cell_matches_search(
                                                    value.as_ref(),
                                                    query,
                                                    *match_case,
                                                    *whole_word,
                                                )
                                        },
                                    );
                                    let is_cell_selected =
                                        cell_selection.is_some_and(|selection| {
                                            selection.contains(display_row, index)
                                        });
                                    let is_editing = editing_cell.as_ref().is_some_and(|editing| {
                                        editing.display_row == display_row
                                            && editing.source_row
                                                == source_row.unwrap_or(display_row)
                                            && editing.column == index
                                    });
                                    if is_editing {
                                        body_cell_frame(width, cx)
                                            .id(SharedString::from(format!(
                                                "cell-{display_row}-{index}-editor"
                                            )))
                                            .border_1()
                                            .border_color(cx.theme().accent)
                                            .child(
                                                Input::new(&edit_input)
                                                    .appearance(false)
                                                    .bordered(false)
                                                    .focus_bordered(false)
                                                    .disabled(this.operation.is_running()),
                                            )
                                            .into_any_element()
                                    } else {
                                        let editor_value = value.clone();
                                        let keyboard_value = value.clone();
                                        let context_value = value.clone();
                                        let source_row = source_row.unwrap_or(display_row);
                                        body_cell(value, width, cx)
                                            .when(is_cell_match, |cell| {
                                                cell.bg(cx.theme().info.opacity(0.32))
                                                    .border_1()
                                                    .border_color(cx.theme().info)
                                            })
                                            .when(is_cell_selected, |cell| {
                                                cell.bg(cx.theme().selection)
                                                    .border_1()
                                                    .border_color(cx.theme().accent)
                                            })
                                            .id(SharedString::from(format!(
                                                "cell-{display_row}-{index}"
                                            )))
                                            .on_mouse_down(
                                                MouseButton::Left,
                                                cx.listener(
                                                    move |this, event: &MouseDownEvent, _, cx| {
                                                        this.begin_cell_selection(
                                                            display_row,
                                                            index,
                                                            event,
                                                            cx,
                                                        );
                                                        cx.stop_propagation();
                                                    },
                                                ),
                                            )
                                            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                                                this.drag_cell_selection(display_row, index, cx);
                                            }))
                                            .on_click(|_, _, cx| cx.stop_propagation())
                                            .when(can_edit, |cell| {
                                                cell.cursor_text()
                                                .tab_index(0)
                                                .on_click(cx.listener(
                                                    move |this,
                                                          event: &ClickEvent,
                                                          window,
                                                          cx| {
                                                        if event.click_count() == 2 {
                                                            this.begin_cell_edit(
                                                                display_row,
                                                                source_row,
                                                                index,
                                                                editor_value.to_string(),
                                                                window,
                                                                cx,
                                                            );
                                                        }
                                                    },
                                                ))
                                                .on_key_down(cx.listener(
                                                    move |this,
                                                          event: &KeyDownEvent,
                                                          window,
                                                          cx| {
                                                        if event.keystroke.key.as_str() == "enter" {
                                                            cx.stop_propagation();
                                                            this.begin_cell_edit(
                                                                display_row,
                                                                source_row,
                                                                index,
                                                                keyboard_value.to_string(),
                                                                window,
                                                                cx,
                                                            );
                                                        }
                                                    },
                                                ))
                                            })
                                            .on_mouse_down(
                                                MouseButton::Right,
                                                cx.listener(
                                                    move |this, event: &MouseDownEvent, _, cx| {
                                                        this.table.context_cell = Some((
                                                            display_row,
                                                            source_row,
                                                            index,
                                                            context_value.to_string(),
                                                        ));
                                                        this.table.table_context_menu =
                                                            Some(TableContextMenu {
                                                                position: event.position,
                                                                kind: TableContextMenuKind::Cell {
                                                                    can_edit,
                                                                },
                                                                focused_item: 0,
                                                            });
                                                        cx.notify();
                                                    },
                                                ),
                                            )
                                            .into_any_element()
                                    }
                                },
                            ))
                    })
                    .collect::<Vec<_>>()
            }),
        )
        .track_scroll(self.table.row_scroll.clone())
        .h_full()
        .w(px(table_width));

        let table_content = v_flex()
            .size_full()
            .pr(px(16.0))
            .pb(px(16.0))
            .overflow_hidden()
            .child(header)
            .child(
                div()
                    .id("table-body-horizontal-scroll")
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_scroll()
                    .track_scroll(&self.table.column_scroll)
                    .child(rows),
            );

        v_flex()
            .size_full()
            .child(
                h_flex()
                    .flex_none()
                    .min_h(px(46.0))
                    .px_3()
                    .py_1()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("toolbar-open")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::FolderOpen)
                                    .tooltip("Open")
                                    .when(show_toolbar_labels, |button| button.label("Open"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.open_dialog(&OpenFile, window, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-save")
                                    .compact()
                                    .icon(IconName::Check)
                                    .label("Save")
                                    .tooltip("Save")
                                    .disabled(!dirty)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_file(&SaveFile, window, cx)
                                    })),
                            ),
                    )
                    .child(toolbar_divider(cx))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("toolbar-copy")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Copy)
                                    .tooltip("Copy")
                                    .when(show_toolbar_labels, |button| button.label("Copy"))
                                    .disabled(self.selection.selected_row.is_none())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.copy_selected(window, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-delete")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Delete)
                                    .tooltip("Delete")
                                    .when(show_toolbar_labels, |button| button.label("Delete"))
                                    .disabled(self.selection.selected_row.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.mutate_selected_rows(RowMutation::Delete, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-restore")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Undo)
                                    .tooltip("Restore")
                                    .when(show_toolbar_labels, |button| button.label("Restore"))
                                    .disabled(self.selection.selected_row.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.mutate_selected_rows(RowMutation::Restore, cx)
                                    })),
                            ),
                    )
                    .child(toolbar_divider(cx))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("toolbar-find")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Search)
                                    .tooltip("Find")
                                    .when(show_toolbar_labels, |button| button.label("Find"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.show_find(&Find, window, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-duplicates")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::GalleryVerticalEnd)
                                    .tooltip("Find duplicates")
                                    .when(show_toolbar_labels, |button| button.label("Dupes"))
                                    .disabled(self.operation.is_running())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.check_duplicates(&CheckDuplicates, window, cx)
                                    })),
                            ),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .px_2()
                            .text_xs()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .text_color(cx.theme().muted_foreground)
                            .whitespace_nowrap()
                            .child(format!("{} rows", format_count(row_count))),
                    )
                    .child(
                        Button::new("toolbar-settings")
                            .compact()
                            .ghost()
                            .icon(IconName::Settings2)
                            .tooltip("Settings")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_settings(&OpenSettings, window, cx)
                            })),
                    ),
            )
            .when(self.queries.show_find, |this| {
                this.child(
                    h_flex()
                        .flex_none()
                        .min_h(px(46.0))
                        .flex_wrap()
                        .px_3()
                        .py_1()
                        .gap_2()
                        .border_l_2()
                        .border_b_1()
                        .border_color(cx.theme().border)
                        .bg(cx.theme().secondary.opacity(0.55))
                        .child(
                            div()
                                .px_2()
                                .text_xs()
                                .font_weight(gpui::FontWeight::BOLD)
                                .text_color(cx.theme().accent)
                                .child("FIND"),
                        )
                        .child(query_scope_dropdown(
                            "search-scope",
                            search_scope,
                            header_labels.clone(),
                            self.queries.search.column,
                            self.operation.is_running(),
                            QueryScopeKind::Search,
                            query_view.clone().expect(
                                "QuickRows view identity is initialized before rendering query controls",
                            ),
                        ))
                        .child(Input::new(&self.inputs.search_input).flex_1().min_w(px(180.0)))
                        .child(
                            Button::new("search-match-case")
                                .compact()
                                .label(if self.queries.search.match_case {
                                    "Aa ✓"
                                } else {
                                    "Aa"
                                })
                                .disabled(self.operation.is_running())
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.toggle_search_match_case(cx)),
                                ),
                        )
                        .child(
                            Button::new("search-whole-word")
                                .compact()
                                .label(if self.queries.search.whole_word {
                                    "ab ✓"
                                } else {
                                    "ab"
                                })
                                .disabled(self.operation.is_running())
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.toggle_search_whole_word(cx)),
                                ),
                        )
                        .child(
                            Button::new("run-search")
                                .compact()
                                .primary()
                                .label("Find")
                                .disabled(self.operation.is_running())
                                .on_click(cx.listener(|this, _, _, cx| this.run_search(cx))),
                        )
                        .child(
                            Button::new("previous-match")
                                .compact()
                                .ghost()
                                .label("↑")
                                .disabled(
                                    self.operation.is_running()
                                        || self.queries.search.stale
                                        || self.queries.search.results.is_empty(),
                                )
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.previous_match(&PreviousMatch, window, cx)
                                })),
                        )
                        .child(
                            Button::new("next-match")
                                .compact()
                                .ghost()
                                .label("↓")
                                .disabled(
                                    self.operation.is_running()
                                        || self.queries.search.stale
                                        || self.queries.search.results.is_empty(),
                                )
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.next_match(&NextMatch, window, cx)
                                })),
                        )
                        .when_some(
                            query_result_label(
                                self.queries.search.current_match,
                                self.queries.search.results.len(),
                                self.queries.search.completed,
                                "No matches",
                            ),
                            |panel, label| panel.child(label),
                        )
                        .when(self.queries.search.stale, |panel| {
                            panel.child("Outdated").child(
                                Button::new("rerun-search")
                                    .compact()
                                    .primary()
                                    .label("Re-run")
                                    .disabled(self.operation.is_running())
                                    .on_click(cx.listener(|this, _, _, cx| this.run_search(cx))),
                            )
                        })
                        .child(
                            Button::new("clear-find")
                                .compact()
                                .ghost()
                                .label("Clear")
                                .disabled(self.queries.search.results.is_empty())
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.clear_search(&ClearSearch, window, cx)
                                })),
                        )
                        .child(
                            Button::new("close-find")
                                .compact()
                                .ghost()
                                .label("Close")
                                .on_click(cx.listener(|this, _, _, cx| this.hide_find(cx))),
                        ),
                )
            })
            .when(self.queries.show_duplicates, |this| {
                this.child(
                    h_flex()
                        .flex_none()
                        .min_h(px(46.0))
                        .flex_wrap()
                        .px_3()
                        .py_1()
                        .gap_2()
                        .border_l_2()
                        .border_b_1()
                        .border_color(cx.theme().border)
                        .bg(cx.theme().secondary.opacity(0.55))
                        .child(
                            div()
                                .px_2()
                                .text_xs()
                                .font_weight(gpui::FontWeight::BOLD)
                                .text_color(cx.theme().accent)
                                .child("DUPLICATES"),
                        )
                        .child(query_scope_dropdown(
                            "duplicate-scope",
                            duplicate_scope,
                            header_labels.clone(),
                            self.queries.duplicates.column,
                            self.operation.is_running(),
                            QueryScopeKind::Duplicates,
                            query_view.clone().expect(
                                "QuickRows view identity is initialized before rendering query controls",
                            ),
                        ))
                        .child(
                            Button::new("run-duplicates")
                                .compact()
                                .primary()
                                .label(if self.operation.is_running() { "Checking…" } else { "Check" })
                                .disabled(self.operation.is_running())
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.run_duplicate_check(cx)),
                                ),
                        )
                        .child(
                            Button::new("clear-duplicates")
                                .compact()
                                .ghost()
                                .label("Clear")
                                .disabled(self.queries.duplicates.results.is_empty())
                                .on_click(cx.listener(|this, _, _, cx| this.clear_duplicates(cx))),
                        )
                        .when_some(
                            query_result_label(
                                self.queries.duplicates.current_match,
                                self.queries.duplicates.results.len(),
                                self.queries.duplicates.completed,
                                "No duplicates",
                            ),
                            |panel, label| panel.child(label),
                        )
                        .when(self.queries.duplicates.stale, |panel| {
                            panel.child("Outdated").child(
                                Button::new("rerun-duplicates")
                                    .compact()
                                    .primary()
                                    .label("Re-run")
                                    .disabled(self.operation.is_running())
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.run_duplicate_check(cx)),
                                    ),
                            )
                        })
                        .child(
                            Button::new("previous-duplicate")
                                .compact()
                                .ghost()
                                .label("↑")
                                .disabled(
                                    self.operation.is_running()
                                        || self.queries.duplicates.stale
                                        || self.queries.duplicates.results.is_empty(),
                                )
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.previous_duplicate(cx)),
                                ),
                        )
                        .child(
                            Button::new("next-duplicate")
                                .compact()
                                .ghost()
                                .label("↓")
                                .disabled(
                                    self.operation.is_running()
                                        || self.queries.duplicates.stale
                                        || self.queries.duplicates.results.is_empty(),
                                )
                                .on_click(cx.listener(|this, _, _, cx| this.next_duplicate(cx))),
                        )
                        .child(
                            Button::new("close-duplicates")
                                .compact()
                                .ghost()
                                .label("Close")
                                .on_click(cx.listener(|this, _, _, cx| this.hide_duplicates(cx))),
                        ),
                )
            })
            .child(
                div()
                    .id("table-viewport")
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(table_content)
                    // ScrollableMask is the GPUI Component table pattern for
                    // separating dominant horizontal trackpad movement from the
                    // virtual list's vertical wheel handling.
                    .child(ScrollableMask::new(Axis::Horizontal, &self.table.column_scroll))
                    .child(
                        div()
                            .id("table-horizontal-scrollbar-track")
                            .occlude()
                            .absolute()
                            .left_0()
                            .right(px(16.0))
                            .bottom_0()
                            .h(px(16.0))
                            .border_t_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary)
                            .child(
                                Scrollbar::horizontal(&self.table.column_scroll)
                                    .id("table-horizontal-scrollbar")
                                    .scrollbar_show(ScrollbarShow::Always),
                            ),
                    )
                    .child(
                        div()
                            .id("table-vertical-scrollbar-track")
                            .occlude()
                            .absolute()
                            .top(px(row_height))
                            .right_0()
                            .bottom(px(16.0))
                            .w(px(16.0))
                            .border_l_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary)
                            .child(
                                Scrollbar::vertical(&self.table.row_scroll)
                                    .id("table-vertical-scrollbar")
                                    .scrollbar_show(ScrollbarShow::Always),
                            ),
                    )
                    .child(
                        div()
                            .absolute()
                            .right_0()
                            .bottom_0()
                            .w(px(16.0))
                            .h(px(16.0))
                            .border_t_1()
                            .border_l_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary),
                    ),
            )
            .child(
                h_flex()
                    .flex_none()
                    .h(px(30.0))
                    .px_3()
                    .gap_3()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().secondary.opacity(0.65))
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(div().w(px(7.0)).h(px(7.0)).rounded_full().bg(if dirty {
                        cx.theme().yellow
                    } else {
                        cx.theme().green
                    }))
                    .child(if dirty { "Unsaved changes" } else { "Saved" })
                    .child("•")
                    .child(format!(
                        "{} delimiter",
                        visible_control(&loaded.parse_info.delimiter)
                    ))
                    .child("•")
                    .child(loaded.parse_info.encoding.clone())
                    .when_some(cell_dimensions, |status, (rows, columns)| {
                        status.child("•").child(format!(
                            "{} × {} cells selected",
                            format_count(rows),
                            format_count(columns)
                        ))
                    })
                    .when(cell_dimensions.is_none() && selected_count > 0, |status| {
                        status
                            .child("•")
                            .child(format!("{} rows selected", format_count(selected_count)))
                    })
                    .child(div().flex_1())
                    .child(if cfg!(target_os = "macos") {
                        "⌘F Find"
                    } else {
                        "Ctrl+F Find"
                    })
                    .child(if cfg!(target_os = "macos") {
                        "⌘, Settings"
                    } else {
                        "Ctrl+, Settings"
                    }),
            )
            .into_any_element()
    }
}
