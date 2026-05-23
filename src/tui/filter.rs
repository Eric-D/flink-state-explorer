use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterMode {
    Pattern,
    Exact,
    KeyGroupRange,
}

impl FilterMode {
    pub fn label(&self) -> &str {
        match self {
            FilterMode::Pattern => "pattern",
            FilterMode::Exact => "exact",
            FilterMode::KeyGroupRange => "kg-range",
        }
    }

    pub fn cycle(&self) -> Self {
        match self {
            FilterMode::Pattern => FilterMode::Exact,
            FilterMode::Exact => FilterMode::KeyGroupRange,
            FilterMode::KeyGroupRange => FilterMode::Pattern,
        }
    }
}

pub struct FilterBar {
    pub active: bool,
    pub mode: FilterMode,
    pub input: String,
    pub cursor: usize,
    /// The currently applied filter (None = show all).
    pub applied: Option<AppliedFilter>,
}

#[derive(Debug, Clone)]
pub enum AppliedFilter {
    /// Glob-style pattern with `*` wildcards. e.g. `TST*`, `*incident*`, `ABC-*-001`
    Pattern(String),
    Exact(Vec<u8>),
    KeyGroupRange {
        start: u16,
        end: u16,
    },
}

impl AppliedFilter {
    /// Check if a key (as string) matches this filter.
    pub fn matches_str(&self, key: &str, key_group: u16) -> bool {
        match self {
            AppliedFilter::Pattern(pattern) => glob_match(pattern, key),
            AppliedFilter::Exact(exact) => key.as_bytes() == exact.as_slice(),
            AppliedFilter::KeyGroupRange { start, end } => key_group >= *start && key_group <= *end,
        }
    }

    /// Check if a key (as bytes) matches this filter.
    pub fn matches_bytes(&self, key: &[u8], key_group: u16) -> bool {
        match self {
            AppliedFilter::Pattern(pattern) => {
                let key_str = String::from_utf8_lossy(key);
                glob_match(pattern, &key_str)
            }
            AppliedFilter::Exact(exact) => key == exact.as_slice(),
            AppliedFilter::KeyGroupRange { start, end } => key_group >= *start && key_group <= *end,
        }
    }
}

/// Simple glob matching: `*` matches any sequence of characters (including empty).
/// Case-insensitive matching.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let text = text.to_lowercase();
    glob_match_inner(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_inner(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'*' {
            // Star: remember position, try matching zero chars first
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if star_pi != usize::MAX {
            // Backtrack: try matching one more char with the star
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Consume trailing stars
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

impl Default for FilterBar {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterBar {
    pub fn new() -> Self {
        Self {
            active: false,
            mode: FilterMode::Pattern,
            input: String::new(),
            cursor: 0,
            applied: None,
        }
    }

    pub fn activate(&mut self) {
        self.active = true;
        self.input.clear();
        self.cursor = 0;
    }

    pub fn deactivate(&mut self) {
        self.active = false;
        self.input.clear();
        self.cursor = 0;
        self.applied = None;
    }

    pub fn cycle_mode(&mut self) {
        self.mode = self.mode.cycle();
    }

    pub fn insert_char(&mut self, c: char) {
        self.input.insert(self.cursor, c);
        self.cursor += c.len_utf8();
    }

    pub fn backspace(&mut self) {
        if self.cursor > 0 {
            let prev = self.input[..self.cursor]
                .char_indices()
                .next_back()
                .map(|(i, _)| i)
                .unwrap_or(0);
            self.input.drain(prev..self.cursor);
            self.cursor = prev;
        }
    }

    pub fn apply(&mut self) {
        if self.input.is_empty() {
            self.applied = None;
            return;
        }

        self.applied = match self.mode {
            FilterMode::Pattern => Some(AppliedFilter::Pattern(self.input.clone())),
            FilterMode::Exact => Some(AppliedFilter::Exact(self.input.as_bytes().to_vec())),
            FilterMode::KeyGroupRange => {
                // Parse "start-end" or "start end"
                let parts: Vec<&str> = self.input.split(['-', ' ']).collect();
                if parts.len() == 2 {
                    if let (Ok(start), Ok(end)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>())
                    {
                        Some(AppliedFilter::KeyGroupRange { start, end })
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        if !self.active && self.applied.is_none() {
            return;
        }

        let status = if self.active {
            format!(" [{}] filter: {}█", self.mode.label(), &self.input)
        } else if let Some(filter) = &self.applied {
            let desc = match filter {
                AppliedFilter::Pattern(p) => {
                    format!("pattern={}", p)
                }
                AppliedFilter::Exact(e) => {
                    format!("exact={}", String::from_utf8_lossy(e))
                }
                AppliedFilter::KeyGroupRange { start, end } => {
                    format!("kg={}-{}", start, end)
                }
            };
            format!(" filter active: {} (Esc to clear)", desc)
        } else {
            return;
        };

        let block = Block::default().borders(Borders::TOP);
        let paragraph = Paragraph::new(status)
            .style(Style::default().fg(Color::Yellow).bg(Color::DarkGray))
            .block(block);
        frame.render_widget(paragraph, area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_simple_prefix() {
        assert!(glob_match("TST*", "TST-12345"));
        assert!(glob_match("TST*", "TST"));
        assert!(!glob_match("TST*", "ABC-12345"));
    }

    #[test]
    fn glob_contains() {
        assert!(glob_match("*incident*", "abc-incident-123"));
        assert!(glob_match("*incident*", "incident"));
        assert!(!glob_match("*incident*", "abc-other-123"));
    }

    #[test]
    fn glob_suffix() {
        assert!(glob_match("*-001", "TST-001"));
        assert!(!glob_match("*-001", "TST-002"));
    }

    #[test]
    fn glob_middle_wildcard() {
        assert!(glob_match("ABC-*-001", "ABC-xyz-001"));
        assert!(glob_match("ABC-*-001", "ABC--001"));
        assert!(!glob_match("ABC-*-001", "ABC-xyz-002"));
    }

    #[test]
    fn glob_no_wildcard_is_exact() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "hello-world"));
    }

    #[test]
    fn glob_case_insensitive() {
        assert!(glob_match("tst*", "TST-123"));
        assert!(glob_match("TST*", "tst-123"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_match("TST-?", "TST-A"));
        assert!(!glob_match("TST-?", "TST-AB"));
    }
}
