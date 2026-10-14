use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Terminal;

use crate::error::{AppError, ParseError, UiError};
use crate::parser;
use crate::profile::storage::{self as profile_storage, Profile};
use crate::reader::StateReader;
use crate::tui::detail::DetailOverlay;
use crate::tui::filter::FilterBar;
use crate::tui::jobs::{CountMsg, CountTarget, ExportMsg, Job};
use crate::tui::loading::LoadingStep;
use crate::tui::profile_picker::{ProfileAction, ProfilePicker};
use crate::tui::tree::{KeyCount, TreeView};

/// Options for [`App::open`].
#[derive(Debug, Clone, Default)]
pub struct AppOptions {
    /// Where the key counts cache lives (`None`: no cache, neither read nor written).
    pub cache_dir: Option<PathBuf>,
    /// Start the background count pass once the tree is up.
    pub count_keys: bool,
    /// Stored bytes one count step reads (0: [`crate::tui::jobs::COUNT_STEP_BYTES`]).
    pub count_step_bytes: u64,
}

impl AppOptions {
    /// What `flink-explorer` runs with: counts cached under the user cache dir unless
    /// `no_cache`, and counted in the background.
    pub fn from_env(no_cache: bool) -> AppOptions {
        AppOptions {
            cache_dir: if no_cache {
                None
            } else {
                crate::tui::counts::default_cache_dir()
            },
            count_keys: true,
            count_step_bytes: crate::tui::jobs::COUNT_STEP_BYTES,
        }
    }
}

/// Main application state.
pub struct App {
    savepoint_dir: PathBuf,
    checkpoint_id: i64,
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
    options: AppOptions,
    /// The background count pass, while it runs.
    count_job: Option<Job<CountMsg>>,
    /// Counts completed exactly, by operator hex id (saved to the cache once all are).
    counted: Vec<(String, usize)>,
    /// False once the pass met an unreadable key-group: the counts are then not cached.
    count_complete: bool,
    export_job: Option<Job<ExportMsg>>,
}

impl App {
    /// Open `savepoint_dir` with what `flink-explorer` runs with ([`AppOptions::from_env`]),
    /// reporting each loading step to `on_step`.
    pub fn new_with_progress<F>(
        savepoint_dir: PathBuf,
        no_cache: bool,
        on_step: F,
    ) -> Result<Self, AppError>
    where
        F: FnMut(LoadingStep),
    {
        Self::open(savepoint_dir, AppOptions::from_env(no_cache), on_step)
    }

    /// Open `savepoint_dir`: parse `_metadata`, read the operator states and each keyed
    /// operator's headers — nothing else, so the first frame comes right after. Keys are read
    /// when an operator is expanded; counts come from the cache, or from a background pass.
    pub fn open<F>(
        savepoint_dir: PathBuf,
        options: AppOptions,
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

        on_step(LoadingStep::ParsingMetadata);
        let savepoint = parser::parse_metadata(&metadata_path)?;
        let checkpoint_id = savepoint.checkpoint_id;

        on_step(LoadingStep::LoadingProtos);
        let active_profile = profile_storage::load_config_as_profile().ok().or_else(|| {
            profile_storage::load_config()
                .ok()
                .and_then(|gc| gc.last_profile)
                .and_then(|name| profile_storage::load_profile(&name).ok())
        });
        let mut proto_list_entries = Vec::new();
        let mut proto = None;
        if let Some(ref profile) = active_profile {
            if let Some((ctx, loaded_paths)) = build_proto_context(profile) {
                proto = Some(Arc::new(ctx));
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

        let summaries = crate::operators::summarize(&savepoint, &savepoint_dir);
        let mut reader = StateReader::from_savepoint(savepoint, &savepoint_dir);
        if let Some(ctx) = &proto {
            reader = reader.with_proto(ctx.clone());
        }
        // Each operator's headers, one operator at a time (a Ctrl-C between two ends the load):
        // the reader keeps them, the tree only asks what they say.
        let total_operators = summaries.len();
        for (i, s) in summaries.iter().enumerate() {
            on_step(LoadingStep::ReadingHeaders {
                total_operators,
                current_operator: i + 1,
                operator_name: s.display_name.clone(),
            });
            let _ = reader.states(&s.operator_id_hex());
        }
        let mut tree_view = TreeView::new(Arc::new(reader), summaries);
        if let Some(ctx) = proto {
            tree_view.note_proto_context(ctx);
        }

        let mut app = Self {
            savepoint_dir,
            checkpoint_id,
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
            summary_entries: Vec::new(),
            summary_scroll: 0,
            options,
            count_job: None,
            counted: Vec::new(),
            count_complete: true,
            export_job: None,
        };
        app.load_cached_counts();
        if app.options.count_keys {
            app.start_count();
        }
        Ok(app)
    }

    /// The tree (tests drive it directly).
    pub fn tree(&mut self) -> &mut TreeView {
        &mut self.tree_view
    }

    /// Counts from the cache, when it is fresh: the count pass then skips those operators.
    fn load_cached_counts(&mut self) {
        let Some(dir) = &self.options.cache_dir else {
            return;
        };
        let Some(counts) = crate::tui::counts::load(dir, &self.savepoint_dir) else {
            return;
        };
        for (op, hex, _) in self.tree_view.keyed_operators() {
            if let Some(&n) = counts.get(&hex) {
                self.tree_view.set_key_count(op, KeyCount::Exact(n));
                self.counted.push((hex, n));
            }
        }
    }

    /// Start the background count pass over the keyed operators not counted yet.
    pub fn start_count(&mut self) {
        if self.count_job.as_ref().is_some_and(|j| j.is_running()) {
            return;
        }
        let targets: Vec<CountTarget> = self
            .tree_view
            .keyed_operators()
            .into_iter()
            .filter(|(op, _, _)| !matches!(self.tree_view.key_count(*op), Some(KeyCount::Exact(_))))
            .map(|(op, hex, max_parallelism)| CountTarget {
                op,
                hex,
                max_parallelism,
            })
            .collect();
        if targets.is_empty() {
            return;
        }
        self.count_complete = true;
        let step = match self.options.count_step_bytes {
            0 => crate::tui::jobs::COUNT_STEP_BYTES,
            n => n,
        };
        self.count_job = Some(crate::tui::jobs::start_count(
            self.tree_view.reader().clone(),
            targets,
            step,
        ));
    }

    /// Stop the count pass: what it counted so far stays, as a lower bound.
    pub fn cancel_count(&mut self) {
        if let Some(mut job) = self.count_job.take() {
            job.cancel();
            for m in job.drain() {
                self.apply_count(m);
            }
            for (op, _, _) in self.tree_view.keyed_operators() {
                if let Some(KeyCount::Counting { keys, .. }) = self.tree_view.key_count(op) {
                    self.tree_view.set_key_count(op, KeyCount::AtLeast(keys));
                }
            }
        }
    }

    /// Whether the count pass is running.
    pub fn counting(&self) -> bool {
        self.count_job.as_ref().is_some_and(|j| j.is_running())
    }

    /// Whether a background job (count or export) is running, or a search.
    pub fn busy(&self) -> bool {
        self.counting()
            || self.export_job.as_ref().is_some_and(|j| j.is_running())
            || self.tree_view.searching()
    }

    /// Wait for the count pass to finish, then apply what it found (tests).
    pub fn wait_for_count(&mut self) {
        if let Some(job) = self.count_job.as_mut() {
            job.join();
        }
        self.poll_jobs();
    }

    /// Wait for the export to finish, then apply what it reported (tests).
    pub fn wait_for_export(&mut self) {
        if let Some(job) = self.export_job.as_mut() {
            job.join();
        }
        self.poll_jobs();
    }

    /// The status line's message, if any.
    pub fn status(&self) -> Option<&str> {
        self.status_message.as_deref()
    }

    fn apply_count(&mut self, m: CountMsg) {
        match m {
            CountMsg::Progress { op, keys, percent } => {
                self.tree_view
                    .set_key_count(op, KeyCount::Counting { keys, percent });
            }
            CountMsg::Done { op, keys, complete } => {
                if complete {
                    self.tree_view.set_key_count(op, KeyCount::Exact(keys));
                    if let Some(s) = self.tree_view.summary(op) {
                        self.counted.push((s.operator_id_hex(), keys));
                    }
                } else {
                    self.count_complete = false;
                    self.tree_view.set_key_count(op, KeyCount::AtLeast(keys));
                }
            }
        }
    }

    /// Apply what the background jobs reported since the last call.
    pub fn poll_jobs(&mut self) {
        if let Some(job) = &self.count_job {
            let finished = !job.is_running();
            let msgs = job.drain();
            for m in msgs {
                self.apply_count(m);
            }
            if finished {
                self.count_job = None;
                self.save_counts();
            }
        }
        if let Some(job) = &self.export_job {
            let finished = !job.is_running();
            let msgs = job.drain();
            let output = self.export_path();
            for m in msgs {
                match m {
                    ExportMsg::Progress(n) => self.set_status(format!("Exporting… {} keys", n)),
                    ExportMsg::Done(Ok(s)) if s.errors.is_empty() => {
                        self.set_status(format!("Exported {} keys to {}", s.keys, output.display()))
                    }
                    ExportMsg::Done(Ok(s)) => self.set_status(format!(
                        "Exported {} keys to {}; {} unreadable file(s)/key-group(s): their keys \
                         are missing (load_errors)",
                        s.keys,
                        output.display(),
                        s.errors.len()
                    )),
                    ExportMsg::Done(Err(e)) => self.set_status(format!("Export failed: {}", e)),
                }
            }
            if finished {
                self.export_job = None;
            }
        }
    }

    /// Save the counts when every keyed operator was counted exactly and nothing was unreadable
    /// (a lower bound must not be served as a count next time).
    fn save_counts(&mut self) {
        let Some(dir) = &self.options.cache_dir else {
            return;
        };
        let keyed = self.tree_view.keyed_operators().len();
        if !self.count_complete || self.counted.len() < keyed || self.tree_view.load_errors() > 0 {
            return;
        }
        let _ = crate::tui::counts::save(dir, &self.savepoint_dir, &self.counted);
    }

    /// One turn of background work, between frames: apply the jobs' reports, and run one step
    /// of the search when one is going on. Returns whether the search goes on.
    pub fn tick(&mut self) -> bool {
        self.poll_jobs();
        self.tree_view.searching()
            && self
                .tree_view
                .search_step(crate::tui::tree::SEARCH_STEP_BYTES)
    }

    /// Draw the whole screen into `frame` (any backend: tests use `TestBackend`).
    pub fn draw(&mut self, frame: &mut Frame) {
        self.render(frame);
    }

    fn export_path(&self) -> PathBuf {
        self.savepoint_dir.join("export.json")
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
        let mut last_draw = std::time::Instant::now() - Duration::from_secs(1);
        loop {
            // A search runs a step per turn; between steps keys are read without waiting, so
            // `q`, Esc and Ctrl-C answer at once. Frames are drawn at most every 50 ms meanwhile.
            let searching = self.tick();
            if !searching || last_draw.elapsed() >= Duration::from_millis(50) {
                terminal
                    .draw(|frame| self.render(frame))
                    .map_err(UiError::Render)?;
                last_draw = std::time::Instant::now();
            }

            let wait = if searching {
                Duration::ZERO
            } else {
                Duration::from_millis(100)
            };
            // One event per turn, so that a held key still lets frames through.
            if event::poll(wait).map_err(UiError::Event)? {
                if let Event::Key(key) = event::read().map_err(UiError::Event)? {
                    if key.kind == KeyEventKind::Press {
                        self.handle_key(key);
                    }
                }
            }

            if self.should_quit {
                self.shutdown();
                return Ok(());
            }
        }
    }

    /// Stop every background job and search (they end at their next step).
    pub fn shutdown(&mut self) {
        self.tree_view.cancel_search();
        if let Some(mut job) = self.count_job.take() {
            job.cancel();
        }
        if let Some(mut job) = self.export_job.take() {
            job.cancel();
        }
    }

    /// Whether the last key handled asked to quit (`q`, or Ctrl-C from anywhere).
    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    /// Handle one key press. The whole event is needed, not just its code: Ctrl-C arrives here as
    /// `c` + CONTROL (raw mode turns it into a key event instead of SIGINT) and must quit from
    /// any overlay or input — or a search or count — where a plain `c` would be typed or ignored.
    pub fn handle_key(&mut self, key: KeyEvent) {
        if is_interrupt(&key) {
            self.should_quit = true;
            self.shutdown();
            return;
        }
        let code = key.code;
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
            KeyCode::Char('q') => {
                self.should_quit = true;
                self.shutdown();
            }
            KeyCode::Char('/') => self.filter.activate(),
            KeyCode::Char('P') => self.profile_picker.open(),
            KeyCode::Char('I') => {
                self.proto_list_visible = true;
                self.proto_list_scroll = 0;
            }
            KeyCode::Char('S') => {
                self.summary_entries = build_summary_entries(self);
                self.summary_visible = true;
                self.summary_scroll = 0;
            }
            KeyCode::Char('C') => {
                if self.counting() {
                    self.cancel_count();
                    self.set_status("Key count stopped".to_string());
                } else {
                    self.start_count();
                    self.set_status("Counting keys…".to_string());
                }
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
                if !self.tree_view.expand() {
                    // A leaf: open the bytes behind it, if any.
                    if let Some(raw) = self.tree_view.selected_raw_bytes() {
                        self.detail.open_raw(raw);
                    }
                }
            }
            KeyCode::PageDown => self.tree_view.page_down(),
            KeyCode::PageUp => self.tree_view.page_up(),
            KeyCode::Char('x') => match self.tree_view.selected_raw_bytes() {
                Some(raw) => self.detail.open_raw(raw),
                None => self.set_status("x: no bytes behind this row".to_string()),
            },
            KeyCode::Char('i') => {
                // Show all info for the selected operator.
                if let Some(op_idx) = self.tree_view.selected_op_idx() {
                    let keyed = self.tree_view.keyed_info(op_idx);
                    if let Some(op) = self.tree_view.summary(op_idx) {
                        let lines = op.info_lines(keyed.as_ref());
                        self.detail.open_info(lines);
                    }
                }
            }
            KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => {
                if self.tree_view.searching() {
                    // Stop the search, keep what it found.
                    self.tree_view.cancel_search();
                } else if self.filter.applied.is_some() {
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

    /// `E`: stream every operator and every key to `export.json`, on a worker thread.
    fn export_json(&mut self) {
        if self.export_job.as_ref().is_some_and(|j| j.is_running()) {
            self.set_status("An export is already running".to_string());
            return;
        }
        let output = self.export_path();
        self.export_job = Some(crate::tui::jobs::start_export(
            self.tree_view.reader().clone(),
            self.tree_view.summaries(),
            output.clone(),
        ));
        self.set_status(format!("Exporting to {}…", output.display()));
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
            self.checkpoint_id,
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
        let searching;
        let default_shortcuts = "q:quit  ↑↓:nav  Enter:expand  x:hex  i:op-info  /:filter  S:summary  C:count  H:hide  D:debug  P:profile  I:protos  E:export";
        let status = if self.detail.visible {
            "↑↓:scroll  Tab:key/value  Esc:close"
        } else if self.filter.active {
            "Tab:mode  Enter:apply  Esc:cancel"
        } else if let (None, Some(p)) = (&self.status_message, self.tree_view.search_percent()) {
            searching = format!("Searching keys… {}%  Esc:stop  q:quit", p);
            &searching
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

/// Ctrl-C, as a key event: in raw mode the terminal sends it to us instead of raising SIGINT.
/// Consume every event in `events` and say whether one was a Ctrl-C press. The others are
/// dropped on purpose: keys typed while the index loads would otherwise be replayed on a tree the
/// user has not seen yet. `main` feeds it the events queued at each loading step.
pub fn drain_is_interrupt(events: impl IntoIterator<Item = crossterm::event::Event>) -> bool {
    let mut interrupted = false;
    for event in events {
        if let crossterm::event::Event::Key(key) = event {
            if key.kind == crossterm::event::KeyEventKind::Press && is_interrupt(&key) {
                interrupted = true;
            }
        }
    }
    interrupted
}

pub fn is_interrupt(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
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
pub fn build_proto_context(profile: &Profile) -> Option<(crate::proto::ProtoContext, Vec<String>)> {
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

fn build_summary_entries(app: &App) -> Vec<String> {
    let tree = &app.tree_view;
    let reader = tree.reader();
    let savepoint = reader.savepoint();
    let mut lines = Vec::new();

    let ops: Vec<usize> = (0..tree.operator_count()).collect();
    let summary = |i: usize| tree.summary(i).expect("in range");
    let keyed: Vec<usize> = tree.keyed_operators().into_iter().map(|t| t.0).collect();
    let total_known = keyed
        .iter()
        .all(|&op| matches!(tree.key_count(op), Some(KeyCount::Exact(_))));
    let total_keys: usize = keyed.iter().map(|&op| tree.keys_at_least(op)).sum();
    let keys_text = |n: usize, exact: bool| {
        if exact {
            n.to_string()
        } else {
            format!("≥ {}", n)
        }
    };
    let infos: Vec<_> = ops.iter().map(|&i| tree.keyed_info(i)).collect();
    let total_states: usize = infos.iter().flatten().map(|k| k.state_names.len()).sum();
    let total_data_size: u64 = ops.iter().map(|&i| summary(i).keyed_state_size).sum();
    let finished_count = ops.iter().filter(|&&i| summary(i).fully_finished).count();
    let coordinator_count = ops
        .iter()
        .filter(|&&i| summary(i).has_coordinator_state)
        .count();

    // Count state types
    let (mut value_count, mut map_count, mut list_count, mut other_count) = (0, 0, 0, 0);
    for &op in &keyed {
        let hex = summary(op).operator_id_hex();
        if let Ok(states) = reader.states(&hex) {
            for s in &states.states {
                match s.state_type.as_str() {
                    "VALUE" => value_count += 1,
                    "MAP" => map_count += 1,
                    "LIST" => list_count += 1,
                    _ => other_count += 1,
                }
            }
        }
    }

    let max_par: i32 = ops
        .iter()
        .map(|&i| summary(i).parallelism)
        .max()
        .unwrap_or(0);
    let max_max_par: i32 = ops
        .iter()
        .map(|&i| summary(i).max_parallelism)
        .max()
        .unwrap_or(0);
    let metadata_size = std::fs::metadata(app.savepoint_dir.join("_metadata"))
        .map(|m| m.len())
        .unwrap_or(0);

    lines.push("--- Savepoint ---".to_string());
    lines.push(format!("  Format version:     {}", savepoint.version));
    lines.push(format!("  Checkpoint ID:      {}", savepoint.checkpoint_id));
    lines.push(format!(
        "  Metadata size:      {}",
        format_size(metadata_size)
    ));
    lines.push(format!(
        "  State data size:    {}",
        format_size(total_data_size)
    ));
    lines.push(String::new());

    lines.push("--- Operators ---".to_string());
    lines.push(format!("  Total:              {}", ops.len()));
    lines.push(format!("  With keyed state:   {}", keyed.len()));
    lines.push(format!("  Fully finished:     {}", finished_count));
    lines.push(format!("  With coordinator:   {}", coordinator_count));
    lines.push(format!(
        "  Max parallelism:    {} (configured max: {})",
        max_par, max_max_par
    ));
    let unread: usize = infos.iter().flatten().map(|k| k.errors.len()).sum();
    if unread > 0 {
        let with = infos
            .iter()
            .flatten()
            .filter(|k| !k.errors.is_empty())
            .count();
        lines.push(format!(
            "  ⚠ Unread files:     {} in {} operator(s): keys are missing (i: details)",
            unread, with
        ));
    }
    lines.push(String::new());

    lines.push("--- Keyed State ---".to_string());
    lines.push(format!(
        "  Total keys:         {}{}",
        keys_text(total_keys, total_known),
        if app.counting() {
            " (counting…)"
        } else if total_known {
            ""
        } else {
            " (C: count them all)"
        }
    ));
    lines.push(format!(
        "  Registered states:  {} (VALUE:{} MAP:{} LIST:{} other:{})",
        total_states, value_count, map_count, list_count, other_count
    ));
    lines.push(String::new());

    if !savepoint.master_states.is_empty() {
        lines.push("--- Master / Coordinator States ---".to_string());
        for m in &savepoint.master_states {
            lines.push(format!(
                "  {} (v{}, {} bytes)",
                m.name,
                m.version,
                m.data.len()
            ));
        }
        lines.push(String::new());
    }

    // Top operators by key count
    let mut ops_by_keys: Vec<(usize, usize)> = keyed
        .iter()
        .map(|&op| (op, tree.keys_at_least(op)))
        .filter(|(_, k)| *k > 0)
        .collect();
    ops_by_keys.sort_by_key(|o| std::cmp::Reverse(o.1));
    let shown_keys =
        |op: usize, n: usize| keys_text(n, matches!(tree.key_count(op), Some(KeyCount::Exact(_))));

    if !ops_by_keys.is_empty() {
        lines.push("--- Top Operators by Keys ---".to_string());
        for (op, keys) in ops_by_keys.iter().take(15) {
            let s = summary(*op);
            lines.push(format!(
                "  {:>8} keys  {:>8}  {}",
                shown_keys(*op, *keys),
                format_size(s.keyed_state_size),
                s.display_name
            ));
        }
        lines.push(String::new());
    }

    // Top operators by data size
    let mut ops_by_size: Vec<usize> = ops
        .iter()
        .copied()
        .filter(|&i| summary(i).keyed_state_size > 0)
        .collect();
    ops_by_size.sort_by(|a, b| {
        summary(*b)
            .keyed_state_size
            .cmp(&summary(*a).keyed_state_size)
    });

    if !ops_by_size.is_empty() {
        lines.push("--- Top Operators by Data Size ---".to_string());
        for &op in ops_by_size.iter().take(15) {
            let s = summary(op);
            lines.push(format!(
                "  {:>8}  {:>8} keys  {}",
                format_size(s.keyed_state_size),
                shown_keys(op, tree.keys_at_least(op)),
                s.display_name
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

impl Drop for App {
    fn drop(&mut self) {
        self.shutdown();
    }
}
