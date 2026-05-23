use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};

use crate::profile::storage;

#[derive(Debug, Clone, Copy, PartialEq)]
enum PickerMode {
    List,
    CreateName,
    CreateProtoDir,
    ConfirmDelete,
}

pub struct ProfilePicker {
    pub visible: bool,
    mode: PickerMode,
    profiles: Vec<String>,
    /// "name (source)" for display
    profile_labels: Vec<String>,
    /// Full path of each profile's effective file
    profile_paths: Vec<String>,
    list_state: ListState,
    input: String,
    new_name: String,
    pub selected_profile: Option<String>,
    /// Config layers info for display
    config_layers: String,
}

impl Default for ProfilePicker {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfilePicker {
    pub fn new() -> Self {
        Self {
            visible: false,
            mode: PickerMode::List,
            profiles: Vec::new(),
            profile_labels: Vec::new(),
            profile_paths: Vec::new(),
            list_state: ListState::default(),
            input: String::new(),
            new_name: String::new(),
            selected_profile: None,
            config_layers: String::new(),
        }
    }

    pub fn open(&mut self) {
        self.profiles = storage::list_profiles().unwrap_or_default();
        let dirs = storage::config_dirs();
        self.config_layers = dirs
            .iter()
            .map(|d| d.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(" + ");
        self.profile_paths = self
            .profiles
            .iter()
            .map(|name| {
                storage::profile_file_path(name)
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        self.profile_labels = self
            .profiles
            .iter()
            .map(|name| {
                // Show which layer(s) the profile comes from
                let mut sources = Vec::new();
                for dir in &dirs {
                    let path = dir.join("profiles").join(format!("{}.toml", name));
                    if path.exists() {
                        if dir.starts_with(storage::binary_dir()) {
                            sources.push("bin");
                        } else {
                            sources.push("local");
                        }
                    }
                }
                if sources.len() > 1 {
                    format!("{} [{}]", name, sources.join("+"))
                } else if sources.first() == Some(&"local") {
                    format!("{} [local]", name)
                } else {
                    name.clone()
                }
            })
            .collect();
        self.mode = PickerMode::List;
        self.visible = true;
        self.input.clear();
        if !self.profiles.is_empty() {
            self.list_state.select(Some(0));
        }
    }

    pub fn close(&mut self) {
        self.visible = false;
    }

    pub fn handle_key(&mut self, code: crossterm::event::KeyCode) -> Option<ProfileAction> {
        use crossterm::event::KeyCode;
        match self.mode {
            PickerMode::List => match code {
                KeyCode::Esc => {
                    self.close();
                    None
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if let Some(sel) = self.list_state.selected() {
                        if sel > 0 {
                            self.list_state.select(Some(sel - 1));
                        }
                    }
                    None
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if let Some(sel) = self.list_state.selected() {
                        if sel + 1 < self.profiles.len() {
                            self.list_state.select(Some(sel + 1));
                        }
                    }
                    None
                }
                KeyCode::Enter => {
                    if let Some(sel) = self.list_state.selected() {
                        if sel < self.profiles.len() {
                            let name = self.profiles[sel].clone();
                            self.selected_profile = Some(name.clone());
                            self.close();
                            return Some(ProfileAction::Select(name));
                        }
                    }
                    None
                }
                KeyCode::Char('n') => {
                    self.mode = PickerMode::CreateName;
                    self.input.clear();
                    None
                }
                KeyCode::Char('D') => {
                    if self.list_state.selected().is_some() {
                        self.mode = PickerMode::ConfirmDelete;
                    }
                    None
                }
                _ => None,
            },
            PickerMode::CreateName => match code {
                KeyCode::Esc => {
                    self.mode = PickerMode::List;
                    None
                }
                KeyCode::Enter => {
                    if !self.input.is_empty() {
                        self.new_name = self.input.clone();
                        self.input.clear();
                        self.mode = PickerMode::CreateProtoDir;
                    }
                    None
                }
                KeyCode::Backspace => {
                    self.input.pop();
                    None
                }
                KeyCode::Char(c) => {
                    self.input.push(c);
                    None
                }
                _ => None,
            },
            PickerMode::CreateProtoDir => match code {
                KeyCode::Esc => {
                    self.mode = PickerMode::List;
                    None
                }
                KeyCode::Enter => {
                    let proto_dir = self.input.clone();
                    let name = self.new_name.clone();
                    self.input.clear();
                    self.mode = PickerMode::List;
                    self.close();
                    Some(ProfileAction::Create {
                        name,
                        proto_dir: std::path::PathBuf::from(proto_dir),
                    })
                }
                KeyCode::Backspace => {
                    self.input.pop();
                    None
                }
                KeyCode::Char(c) => {
                    self.input.push(c);
                    None
                }
                _ => None,
            },
            PickerMode::ConfirmDelete => match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    if let Some(sel) = self.list_state.selected() {
                        if sel < self.profiles.len() {
                            let name = self.profiles[sel].clone();
                            self.mode = PickerMode::List;
                            return Some(ProfileAction::Delete(name));
                        }
                    }
                    self.mode = PickerMode::List;
                    None
                }
                _ => {
                    self.mode = PickerMode::List;
                    None
                }
            },
        }
    }

    pub fn refresh(&mut self) {
        self.profiles = storage::list_profiles().unwrap_or_default();
        if self.profiles.is_empty() {
            self.list_state.select(None);
        } else if let Some(sel) = self.list_state.selected() {
            if sel >= self.profiles.len() {
                self.list_state.select(Some(self.profiles.len() - 1));
            }
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        if !self.visible {
            return;
        }

        let popup = centered_rect(60, 50, area);
        frame.render_widget(Clear, popup);

        let title = match self.mode {
            PickerMode::List => " Profiles (n:new  D:delete  Enter:select  Esc:close) ",
            PickerMode::CreateName => " New Profile — Enter name: ",
            PickerMode::CreateProtoDir => " New Profile — Proto directory path: ",
            PickerMode::ConfirmDelete => " Delete profile? (y/n) ",
        };
        // Show full path of selected profile at bottom
        let selected_path = self
            .list_state
            .selected()
            .and_then(|i| self.profile_paths.get(i))
            .cloned()
            .unwrap_or_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .title_bottom(
                Line::from(format!(" {} ", self.config_layers))
                    .style(Style::default().fg(Color::DarkGray))
                    .left_aligned(),
            )
            .style(Style::default().bg(Color::Black));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);

        match self.mode {
            PickerMode::List | PickerMode::ConfirmDelete => {
                // Split inner: list + path display at bottom
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Min(1), Constraint::Length(1)])
                    .split(inner);

                let items: Vec<ListItem> = self
                    .profile_labels
                    .iter()
                    .map(|name| ListItem::new(format!("  {}", name)))
                    .collect();
                let list = List::new(items)
                    .highlight_style(
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    )
                    .highlight_symbol("▶ ");
                let mut state = self.list_state.clone();
                frame.render_stateful_widget(list, chunks[0], &mut state);

                // Show selected profile full path
                let path_line =
                    Paragraph::new(selected_path).style(Style::default().fg(Color::DarkGray));
                frame.render_widget(path_line, chunks[1]);
            }
            PickerMode::CreateName | PickerMode::CreateProtoDir => {
                let text = format!("{}█", self.input);
                let para = Paragraph::new(text).style(Style::default().fg(Color::Yellow));
                frame.render_widget(para, inner);
            }
        }
    }
}

pub enum ProfileAction {
    Select(String),
    Create {
        name: String,
        proto_dir: std::path::PathBuf,
    },
    Delete(String),
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
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
