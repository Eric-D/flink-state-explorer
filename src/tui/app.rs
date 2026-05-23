use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Terminal;

use crate::error::{AppError, ParseError, UiError};
use crate::index::builder::build_index_with_progress;
use crate::index::cache::{load_cache, save_cache, CachedOperator, IndexCache};
use crate::parser;
use crate::profile::storage::{self as profile_storage, Profile};
use crate::tui::detail::DetailOverlay;
use crate::tui::filter::FilterBar;
use crate::tui::loading::LoadingStep;
use crate::tui::profile_picker::{ProfileAction, ProfilePicker};
use crate::tui::tree::TreeView;

/// Main application state.
pub struct App {
    savepoint_dir: PathBuf,
    index: IndexCache,
    tree_view: TreeView,
    filter: FilterBar,
    detail: DetailOverlay,
    profile_picker: ProfilePicker,
    active_profile: Option<Profile>,
    should_quit: bool,
    status_message: Option<String>,
    status_message_at: Option<std::time::Instant>,
    proto_list_visible: bool,
    proto_list_entries: Vec<String>,
    proto_list_scroll: usize,
    summary_visible: bool,
    summary_entries: Vec<String>,
    summary_scroll: usize,
}

impl App {
    /// Create a new App with progress callbacks for each loading phase.
    pub fn new_with_progress<F>(
        savepoint_dir: PathBuf,
        no_cache: bool,
        mut on_step: F,
    ) -> Result<Self, AppError>
    where
        F: FnMut(LoadingStep),
    {
        on_step(LoadingStep::CheckingMetadata);
        let metadata_path = savepoint_dir.join("_metadata");
        if !metadata_path.exists() {
            return Err(AppError::Parse(ParseError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("No _metadata file found in {}", savepoint_dir.display()),
            ))));
        }
        let (metadata_size, metadata_mtime) = get_file_stats(&metadata_path);

        on_step(LoadingStep::LoadingCache);
        let index = if !no_cache {
            match load_cache(&savepoint_dir, metadata_size, metadata_mtime) {
                Ok(cache) => cache,
                Err(_) => parse_and_build(
                    &metadata_path,
                    metadata_size,
                    metadata_mtime,
                    &savepoint_dir,
                    &mut on_step,
                )?,
            }
        } else {
            parse_and_build(
                &metadata_path,
                metadata_size,
                metadata_mtime,
                &savepoint_dir,
                &mut on_step,
            )?
        };

        on_step(LoadingStep::BuildingTree);
        let mut tree_view = TreeView::new(&index);

        on_step(LoadingStep::LoadingProtos);
        let active_profile = profile_storage::load_config_as_profile().ok().or_else(|| {
            profile_storage::load_config()
                .ok()
                .and_then(|gc| gc.last_profile)
                .and_then(|name| profile_storage::load_profile(&name).ok())
        });

        let mut proto_list_entries = Vec::new();
        if let Some(ref profile) = active_profile {
            if let Some((ctx, loaded_paths)) = build_proto_context(profile) {
                tree_view.set_proto_context(std::sync::Arc::new(ctx));
                proto_list_entries = loaded_paths;
            }
        }
        if proto_list_entries.is_empty() {
            let dirs = profile_storage::config_dirs();
            proto_list_entries.push("No protos loaded.".to_string());
            proto_list_entries.push(String::new());
            proto_list_entries.push("--- Config search paths ---".to_string());
            for d in &dirs {
                proto_list_entries.push(format!("  {}", d.to_string_lossy()));
            }
            proto_list_entries.push(String::new());
            proto_list_entries.push("--- Setup ---".to_string());
            proto_list_entries.push("1. Create config/profiles/<name>.toml".to_string());
            proto_list_entries.push("2. Set proto_dirs = [\"./protos\"]".to_string());
            proto_list_entries.push("3. Place .proto files in the directory".to_string());
            proto_list_entries
                .push("4. google/protobuf/* are built-in (no setup needed)".to_string());
            proto_list_entries.push("5. Select profile with P key".to_string());
        }

        let summary_entries = build_summary_entries(&index);

        Ok(Self {
            savepoint_dir,
            index,
            tree_view,
            filter: FilterBar::new(),
            detail: DetailOverlay::new(),
            profile_picker: ProfilePicker::new(),
            active_profile,
            should_quit: false,
            status_message: None,
            status_message_at: None,
            proto_list_visible: false,
            proto_list_entries,
            proto_list_scroll: 0,
            summary_visible: false,
            summary_entries,
            summary_scroll: 0,
        })
    }

    /// Run the TUI event loop on an already-initialized terminal.
    pub fn run_with_terminal(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> Result<(), AppError> {
        self.event_loop(terminal)
    }

    fn event_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> Result<(), AppError> {
        loop {
            terminal
                .draw(|frame| self.render(frame))
                .map_err(UiError::Render)?;

            if event::poll(Duration::from_millis(100)).map_err(UiError::Event)? {
                if let Event::Key(key) = event::read().map_err(UiError::Event)? {
                    if key.kind == KeyEventKind::Press {
                        self.handle_key(key.code);
                    }
                }
            }

            if self.should_quit {
                return Ok(());
            }
        }
    }

    fn handle_key(&mut self, code: KeyCode) {
        // Profile picker takes priority
        if self.profile_picker.visible {
            if let Some(action) = self.profile_picker.handle_key(code) {
                self.handle_profile_action(action);
            }
            return;
        }

        // Detail overlay takes priority
        if self.detail.visible {
            self.handle_detail_key(code);
            return;
        }

        if self.filter.active {
            self.handle_filter_key(code);
            return;
        }

        // Proto list overlay
        if self.proto_list_visible {
            match code {
                KeyCode::Esc | KeyCode::Char('I') | KeyCode::Char('q') => {
                    self.proto_list_visible = false
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.proto_list_scroll = self.proto_list_scroll.saturating_sub(1)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.proto_list_scroll + 1 < self.proto_list_entries.len() {
                        self.proto_list_scroll += 1;
                    }
                }
                _ => {}
            }
            return;
        }

        // Summary overlay
        if self.summary_visible {
            match code {
                KeyCode::Esc | KeyCode::Char('S') | KeyCode::Char('q') => {
                    self.summary_visible = false
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.summary_scroll = self.summary_scroll.saturating_sub(1)
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.summary_scroll + 1 < self.summary_entries.len() {
                        self.summary_scroll += 1;
                    }
                }
                _ => {}
            }
            return;
        }

        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('/') => self.filter.activate(),
            KeyCode::Char('P') => self.profile_picker.open(),
            KeyCode::Char('I') => {
                self.proto_list_visible = true;
                self.proto_list_scroll = 0;
            }
            KeyCode::Char('S') => {
                self.summary_visible = true;
                self.summary_scroll = 0;
            }
            KeyCode::Char('E') => {
                self.export_json();
            }
            KeyCode::Char('H') => self.tree_view.toggle_hide_empty(),
            KeyCode::Char('D') => {
                self.tree_view.toggle_debug();
                let mode = if self.tree_view.debug_mode {
                    "ON"
                } else {
                    "OFF"
                };
                self.set_status(format!("Debug mode: {}", mode));
            }
            KeyCode::Up | KeyCode::Char('k') => self.tree_view.move_up(),
            KeyCode::Down | KeyCode::Char('j') => self.tree_view.move_down(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if let Some((key, value)) = self.tree_view.expand() {
                    self.detail.open(key, value);
                }
            }
            KeyCode::Char('i') => {
                // Show all info for the selected operator.
                if let Some(op_idx) = self.tree_view.selected_op_idx() {
                    if let Some(op) = self.index.operators.get(op_idx) {
                        self.detail.open_info(op.info_lines());
                    }
                }
            }
            KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => {
                if self.filter.applied.is_some() {
                    self.filter.deactivate();
                    self.tree_view.apply_filter(&None);
                } else {
                    self.tree_view.collapse();
                }
            }
            KeyCode::Home | KeyCode::Char('g') => self.tree_view.jump_first(),
            KeyCode::End | KeyCode::Char('G') => self.tree_view.jump_last(),
            _ => {}
        }
    }

    fn set_status(&mut self, msg: String) {
        self.status_message = Some(msg);
        self.status_message_at = Some(std::time::Instant::now());
    }

    fn export_json(&mut self) {
        let output = self.savepoint_dir.join("export.json");
        let proto_ctx = self.tree_view.proto_context();
        match crate::export::export_json(&self.index, proto_ctx, &output) {
            Ok(count) => {
                self.set_status(format!("Exported {} keys to {}", count, output.display()));
            }
            Err(e) => {
                self.set_status(format!("Export failed: {}", e));
            }
        }
    }

    fn reload_proto_context(&mut self, profile: &Profile) {
        self.proto_list_entries.clear();
        if let Some((ctx, loaded)) = build_proto_context(profile) {
            self.tree_view.set_proto_context(std::sync::Arc::new(ctx));
            self.proto_list_entries = loaded;
        }
        if self.proto_list_entries.is_empty() {
            self.proto_list_entries
                .push("No protos loaded for this profile.".to_string());
        }
    }

    fn handle_profile_action(&mut self, action: ProfileAction) {
        match action {
            ProfileAction::Select(name) => {
                if let Ok(profile) = profile_storage::load_profile(&name) {
                    let _ = profile_storage::set_last_profile(&name);
                    self.set_status(format!("Profile '{}' loaded", name));
                    self.reload_proto_context(&profile);
                    self.active_profile = Some(profile);
                }
            }
            ProfileAction::Create { name, proto_dir } => {
                let profile = Profile {
                    name: name.clone(),
                    proto_dirs: vec![proto_dir],
                    bindings: std::collections::HashMap::new(),
                    proto_patterns: Vec::new(),
                };
                if profile_storage::save_profile(&profile).is_ok() {
                    let _ = profile_storage::set_last_profile(&name);
                    self.set_status(format!("Profile '{}' created", name));
                    self.active_profile = Some(profile);
                }
            }
            ProfileAction::Delete(name) => {
                let _ = profile_storage::delete_profile(&name);
                if self.active_profile.as_ref().is_some_and(|p| p.name == name) {
                    self.active_profile = None;
                }
                self.profile_picker.refresh();
                self.profile_picker.open();
                self.set_status(format!("Profile '{}' deleted", name));
            }
        }
    }

    fn handle_detail_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Esc => self.detail.close(),
            KeyCode::Up | KeyCode::Char('k') => self.detail.scroll_up(),
            KeyCode::Down | KeyCode::Char('j') => self.detail.scroll_down(),
            KeyCode::Tab => self.detail.toggle_panel(),
            _ => {}
        }
    }

    fn handle_filter_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Enter => {
                self.filter.apply();
                self.filter.active = false;
                self.tree_view.apply_filter(&self.filter.applied);
            }
            KeyCode::Esc => {
                self.filter.deactivate();
                self.tree_view.apply_filter(&None);
            }
            KeyCode::Tab => self.filter.cycle_mode(),
            KeyCode::Backspace => self.filter.backspace(),
            KeyCode::Char(c) => self.filter.insert_char(c),
            _ => {}
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();

        let show_filter = self.filter.active || self.filter.applied.is_some();
        let constraints = if show_filter {
            vec![
                Constraint::Min(3),
                Constraint::Length(2),
                Constraint::Length(1),
                Constraint::Length(1),
            ]
        } else {
            vec![
                Constraint::Min(3),
                Constraint::Length(1),
                Constraint::Length(1),
            ]
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area);

        // Main tree view
        let profile_part = self
            .active_profile
            .as_ref()
            .map(|p| format!("[{}] - ", p.name))
            .unwrap_or_default();
        let title = format!(
            " flink-explorer — {}{} (checkpoint {}) ",
            profile_part,
            self.savepoint_dir.display(),
            self.index.checkpoint_id,
        );
        let block = Block::default().borders(Borders::ALL).title(title);
        self.tree_view.render(frame, block, chunks[0]);

        // Filter bar (if active)
        if show_filter {
            self.filter.render(frame, chunks[1]);
        }

        // Detail overlay (on top of tree)
        if self.detail.visible {
            self.detail.render(frame, area);
        }

        // Profile picker (on top of everything)
        if self.profile_picker.visible {
            self.profile_picker.render(frame, area);
        }

        // Proto list overlay
        if self.proto_list_visible {
            self.render_proto_list(frame, area);
        }

        // Summary overlay
        if self.summary_visible {
            self.render_summary(frame, area);
        }

        // Path breadcrumb bar
        let path_idx = if show_filter { 2 } else { 1 };
        let path = self.tree_view.selected_path();
        let path_bar = Paragraph::new(format!(" {}", path))
            .style(Style::default().fg(Color::Cyan).bg(Color::Black));
        frame.render_widget(path_bar, chunks[path_idx]);

        // Expire status message after 3 seconds
        if let Some(at) = self.status_message_at {
            if at.elapsed() > Duration::from_secs(3) {
                self.status_message = None;
                self.status_message_at = None;
            }
        }

        // Status bar
        let status_idx = path_idx + 1;
        let default_shortcuts = "q:quit  ↑↓:nav  Enter:expand  i:op-info  /:filter  S:summary  H:hide  D:debug  P:profile  I:protos  E:export";
        let status = if self.detail.visible {
            "↑↓:scroll  Tab:key/value  Esc:close"
        } else if self.filter.active {
            "Tab:mode  Enter:apply  Esc:cancel"
        } else {
            self.status_message.as_deref().unwrap_or(default_shortcuts)
        };
        let status_bar =
            Paragraph::new(status).style(Style::default().fg(Color::White).bg(Color::DarkGray));
        frame.render_widget(status_bar, chunks[status_idx]);
    }

    fn render_summary(&self, frame: &mut Frame, area: Rect) {
        use ratatui::widgets::Clear;

        let popup = centered_popup(80, 80, area);
        frame.render_widget(Clear, popup);

        let title = " Savepoint Summary  ↑↓:scroll  Esc:close ";
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .style(Style::default().bg(Color::Black));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);

        let visible_height = inner.height as usize;
        let end = (self.summary_scroll + visible_height).min(self.summary_entries.len());
        let start = self.summary_scroll.min(end);

        let items: Vec<ListItem> = self.summary_entries[start..end]
            .iter()
            .map(|entry| {
                let style = if entry.starts_with("---") {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else if entry.starts_with("  ⯈") {
                    Style::default().fg(Color::Cyan)
                } else if entry.starts_with("    ") {
                    Style::default().fg(Color::Gray)
                } else {
                    Style::default().fg(Color::White)
                };
                ListItem::new(entry.as_str()).style(style)
            })
            .collect();
        let list = List::new(items);
        frame.render_widget(list, inner);
    }

    fn render_proto_list(&self, frame: &mut Frame, area: Rect) {
        use ratatui::widgets::Clear;

        let popup = centered_popup(70, 60, area);
        frame.render_widget(Clear, popup);

        let title = format!(
            " Proto Info ({} entries)  ↑↓:scroll  Esc:close ",
            self.proto_list_entries.len()
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .style(Style::default().bg(Color::Black));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);

        let visible_height = inner.height as usize;
        let end = (self.proto_list_scroll + visible_height).min(self.proto_list_entries.len());
        let start = self.proto_list_scroll.min(end);

        let items: Vec<ListItem> = self.proto_list_entries[start..end]
            .iter()
            .map(|entry| {
                let style = if entry.starts_with("---") {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::White)
                };
                ListItem::new(format!("  {}", entry)).style(style)
            })
            .collect();
        let list = List::new(items);
        frame.render_widget(list, inner);
    }
}

fn centered_popup(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let v = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(v[1])[1]
}

/// Returns (ProtoContext, list of loaded proto file paths).
fn build_proto_context(profile: &Profile) -> Option<(crate::proto::ProtoContext, Vec<String>)> {
    if profile.proto_dirs.is_empty()
        && profile.bindings.is_empty()
        && profile.proto_patterns.is_empty()
    {
        return None;
    }
    let resolve_bases = vec![
        crate::profile::storage::binary_dir(),
        std::env::current_dir().unwrap_or_default(),
    ];

    // Track searched paths for diagnostics
    let mut searched_paths: Vec<String> = Vec::new();
    let mut dirs: Vec<std::path::PathBuf> = Vec::new();
    for d in &profile.proto_dirs {
        if d.is_absolute() {
            let found = d.exists();
            searched_paths.push(format!("{} {}", if found { "✓" } else { "✗" }, d.display()));
            if found {
                dirs.push(d.clone());
            }
        } else {
            for base in &resolve_bases {
                let resolved = base.join(d);
                let found = resolved.exists();
                searched_paths.push(format!(
                    "{} {}",
                    if found { "✓" } else { "✗" },
                    resolved.display()
                ));
                if found && !dirs.contains(&resolved) {
                    dirs.push(resolved);
                }
            }
        }
    }

    // Collect all .proto file paths recursively for display
    let mut proto_files: Vec<String> = Vec::new();
    for dir in &dirs {
        for f in crate::proto::compiler::list_proto_files(dir) {
            proto_files.push(f.to_string_lossy().to_string());
        }
    }
    proto_files.sort();

    // Build entries list for display (always includes diagnostics)
    let build_entries = |msg_types: Vec<String>| -> Vec<String> {
        let mut entries = Vec::new();
        entries.push("--- Proto search paths ---".to_string());
        entries.extend(searched_paths.clone());
        entries.push(String::new());
        entries.push(format!("--- Proto files ({}) ---", proto_files.len()));
        entries.extend(proto_files.clone());
        entries.push(String::new());
        let warnings = crate::proto::compiler::last_compile_warnings();
        if !warnings.is_empty() {
            entries.push(format!("--- Warnings ({}) ---", warnings.len()));
            entries.extend(warnings);
            entries.push(String::new());
        }
        entries.push(format!("--- Message types ({}) ---", msg_types.len()));
        entries.extend(msg_types);
        if !profile.proto_patterns.is_empty() {
            entries.push(String::new());
            entries.push(format!(
                "--- Patterns ({}) ---",
                profile.proto_patterns.len()
            ));
            for p in &profile.proto_patterns {
                entries.push(format!(
                    "{{{}.{}}} resolve={}",
                    p.class_field, p.bytes_field, p.resolve
                ));
            }
        }
        entries
    };

    if dirs.is_empty() {
        return Some((
            crate::proto::ProtoContext {
                pool: prost_reflect::DescriptorPool::new(),
                bindings: profile.bindings.clone(),
                patterns: profile.proto_patterns.clone(),
            },
            build_entries(Vec::new()),
        ));
    }

    let dir_refs: Vec<&Path> = dirs.iter().map(|d| d.as_path()).collect();
    match crate::proto::compiler::compile_protos(&dir_refs) {
        Ok(pool) => {
            let msg_types = crate::proto::compiler::list_message_types(&pool);
            let entries = build_entries(msg_types);
            Some((
                crate::proto::ProtoContext {
                    pool,
                    bindings: profile.bindings.clone(),
                    patterns: profile.proto_patterns.clone(),
                },
                entries,
            ))
        }
        Err(e) => {
            let mut entries = build_entries(Vec::new());
            entries.push(String::new());
            entries.push("--- Compilation error ---".to_string());
            entries.push(format!("{}", e));
            Some((
                crate::proto::ProtoContext {
                    pool: prost_reflect::DescriptorPool::new(),
                    bindings: profile.bindings.clone(),
                    patterns: profile.proto_patterns.clone(),
                },
                entries,
            ))
        }
    }
}

fn parse_and_build<F>(
    metadata_path: &Path,
    metadata_size: u64,
    metadata_mtime: u64,
    savepoint_dir: &Path,
    on_step: &mut F,
) -> Result<IndexCache, AppError>
where
    F: FnMut(LoadingStep),
{
    on_step(LoadingStep::ParsingMetadata);
    let savepoint = parser::parse_metadata(metadata_path)?;
    let num_ops = savepoint.operators.len();
    on_step(LoadingStep::BuildingIndex {
        total_operators: num_ops,
        current_operator: 0,
        operator_name: String::new(),
        total_keys: 0,
    });
    let idx = build_index_with_progress(
        &savepoint,
        metadata_size,
        metadata_mtime,
        savepoint_dir,
        |op_idx, op_name, total_keys| {
            on_step(LoadingStep::BuildingIndex {
                total_operators: num_ops,
                current_operator: op_idx + 1,
                operator_name: op_name.to_string(),
                total_keys,
            });
        },
    );
    if let Err(e) = save_cache(&idx, savepoint_dir) {
        eprintln!("Warning: could not save index cache: {}", e);
    }
    Ok(idx)
}

fn build_summary_entries(index: &IndexCache) -> Vec<String> {
    let mut lines = Vec::new();

    let total_operators = index.operators.len();
    let operators_with_keys: usize = index
        .operators
        .iter()
        .filter(|o| o.keyed_state.as_ref().map_or(false, |k| k.total_keys > 0))
        .count();
    let total_keys: usize = index
        .operators
        .iter()
        .filter_map(|o| o.keyed_state.as_ref())
        .map(|k| k.total_keys)
        .sum();
    let total_states: usize = index
        .operators
        .iter()
        .filter_map(|o| o.keyed_state.as_ref())
        .map(|k| k.descriptors.len())
        .sum();
    let total_data_size: u64 = index.operators.iter().map(|o| o.keyed_state_size).sum();
    let finished_count = index.operators.iter().filter(|o| o.fully_finished).count();
    let coordinator_count = index
        .operators
        .iter()
        .filter(|o| o.has_coordinator_state)
        .count();

    // Count state types
    let mut value_count = 0usize;
    let mut map_count = 0usize;
    let mut list_count = 0usize;
    let mut other_count = 0usize;
    for op in &index.operators {
        if let Some(ks) = &op.keyed_state {
            for d in &ks.descriptors {
                match d.state_type.as_str() {
                    "VALUE" => value_count += 1,
                    "MAP" => map_count += 1,
                    "LIST" => list_count += 1,
                    _ => other_count += 1,
                }
            }
        }
    }

    // Parallelism stats
    let max_par: i32 = index
        .operators
        .iter()
        .map(|o| o.parallelism)
        .max()
        .unwrap_or(0);
    let max_max_par: i32 = index
        .operators
        .iter()
        .map(|o| o.max_parallelism)
        .max()
        .unwrap_or(0);

    lines.push("--- Savepoint ---".to_string());
    lines.push(format!("  Format version:     {}", index.savepoint_version));
    lines.push(format!("  Checkpoint ID:      {}", index.checkpoint_id));
    lines.push(format!(
        "  Metadata size:      {}",
        format_size(index.metadata_size)
    ));
    lines.push(format!(
        "  State data size:    {}",
        format_size(total_data_size)
    ));
    lines.push(String::new());

    lines.push("--- Operators ---".to_string());
    lines.push(format!("  Total:              {}", total_operators));
    lines.push(format!("  With keyed state:   {}", operators_with_keys));
    lines.push(format!("  Fully finished:     {}", finished_count));
    lines.push(format!("  With coordinator:   {}", coordinator_count));
    lines.push(format!(
        "  Max parallelism:    {} (configured max: {})",
        max_par, max_max_par
    ));
    lines.push(String::new());

    lines.push("--- Keyed State ---".to_string());
    lines.push(format!("  Total keys:         {}", total_keys));
    lines.push(format!(
        "  Registered states:  {} (VALUE:{} MAP:{} LIST:{} other:{})",
        total_states, value_count, map_count, list_count, other_count
    ));
    lines.push(String::new());

    if !index.master_states.is_empty() {
        lines.push("--- Master / Coordinator States ---".to_string());
        for ms in &index.master_states {
            lines.push(format!("  {}", ms));
        }
        lines.push(String::new());
    }

    // Top operators by key count
    let mut ops_by_keys: Vec<(&CachedOperator, usize)> = index
        .operators
        .iter()
        .filter_map(|o| o.keyed_state.as_ref().map(|ks| (o, ks.total_keys)))
        .filter(|(_, k)| *k > 0)
        .collect();
    ops_by_keys.sort_by(|a, b| b.1.cmp(&a.1));

    if !ops_by_keys.is_empty() {
        lines.push("--- Top Operators by Keys ---".to_string());
        for (op, keys) in ops_by_keys.iter().take(15) {
            let size = format_size(op.keyed_state_size);
            lines.push(format!(
                "  {:>8} keys  {:>8}  {}",
                keys, size, op.display_name
            ));
        }
        lines.push(String::new());
    }

    // Top operators by data size
    let mut ops_by_size: Vec<&CachedOperator> = index
        .operators
        .iter()
        .filter(|o| o.keyed_state_size > 0)
        .collect();
    ops_by_size.sort_by(|a, b| b.keyed_state_size.cmp(&a.keyed_state_size));

    if !ops_by_size.is_empty() {
        lines.push("--- Top Operators by Data Size ---".to_string());
        for op in ops_by_size.iter().take(15) {
            let keys = op.keyed_state.as_ref().map_or(0, |k| k.total_keys);
            lines.push(format!(
                "  {:>8}  {:>8} keys  {}",
                format_size(op.keyed_state_size),
                keys,
                op.display_name
            ));
        }
    }

    lines
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn get_file_stats(path: &Path) -> (u64, u64) {
    match std::fs::metadata(path) {
        Ok(meta) => {
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            (size, mtime)
        }
        Err(_) => (0, 0),
    }
}
