use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Tabs, Wrap};

use crate::tui::hex_view::HexView;

#[derive(Debug, Clone, Copy, PartialEq)]
enum ActivePanel {
    Key,
    Value,
}

pub struct DetailOverlay {
    pub visible: bool,
    key_view: HexView,
    value_view: HexView,
    active_panel: ActivePanel,
    /// When set, the overlay shows operator info text instead of the key/value hex panels.
    info: Option<Vec<(String, String)>>,
}

impl Default for DetailOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl DetailOverlay {
    pub fn new() -> Self {
        Self {
            visible: false,
            key_view: HexView::new(Vec::new(), "key"),
            value_view: HexView::new(Vec::new(), "value"),
            active_panel: ActivePanel::Key,
            info: None,
        }
    }

    pub fn open(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.key_view = HexView::new(key, "key");
        self.value_view = HexView::new(value, "value");
        self.active_panel = ActivePanel::Key;
        self.info = None;
        self.visible = true;
    }

    /// Show operator info (label/value pairs) as text instead of hex panels.
    pub fn open_info(&mut self, lines: Vec<(String, String)>) {
        self.info = Some(lines);
        self.visible = true;
    }

    pub fn close(&mut self) {
        self.visible = false;
        // Drop data
        self.key_view = HexView::new(Vec::new(), "key");
        self.value_view = HexView::new(Vec::new(), "value");
        self.info = None;
    }

    pub fn scroll_up(&mut self) {
        match self.active_panel {
            ActivePanel::Key => self.key_view.scroll_up(),
            ActivePanel::Value => self.value_view.scroll_up(),
        }
    }

    pub fn scroll_down(&mut self) {
        match self.active_panel {
            ActivePanel::Key => self.key_view.scroll_down(),
            ActivePanel::Value => self.value_view.scroll_down(),
        }
    }

    pub fn toggle_panel(&mut self) {
        self.active_panel = match self.active_panel {
            ActivePanel::Key => ActivePanel::Value,
            ActivePanel::Value => ActivePanel::Key,
        };
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        if !self.visible {
            return;
        }

        // Operator info mode: render label/value pairs as text.
        if let Some(info) = &self.info {
            let overlay_area = centered_rect(70, 70, area);
            frame.render_widget(Clear, overlay_area);
            let block = Block::default()
                .borders(Borders::ALL)
                .title(" Operator info (Esc:close) ")
                .style(Style::default().bg(Color::Black));
            let inner = block.inner(overlay_area);
            frame.render_widget(block, overlay_area);
            let lines: Vec<Line> = info
                .iter()
                .map(|(k, v)| {
                    Line::from(vec![
                        Span::styled(
                            format!("{:>18} : ", k),
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(v.clone()),
                    ])
                })
                .collect();
            let para = Paragraph::new(lines).wrap(Wrap { trim: false });
            frame.render_widget(para, inner);
            return;
        }

        // Center overlay: 80% of width, 80% of height
        let overlay_area = centered_rect(80, 80, area);
        frame.render_widget(Clear, overlay_area);

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Entry Detail (Tab:switch  ↑↓:scroll  Esc:close) ")
            .style(Style::default().bg(Color::Black));
        let inner = block.inner(overlay_area);
        frame.render_widget(block, overlay_area);

        // Split inner: tabs header + key panel + value panel
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Percentage(50),
                Constraint::Percentage(50),
            ])
            .split(inner);

        // Tabs
        let titles = vec!["Key", "Value"];
        let selected = match self.active_panel {
            ActivePanel::Key => 0,
            ActivePanel::Value => 1,
        };
        let tabs = Tabs::new(titles)
            .select(selected)
            .style(Style::default().fg(Color::DarkGray))
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_widget(tabs, chunks[0]);

        // Key panel
        let key_style = if self.active_panel == ActivePanel::Key {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let key_block = Block::default()
            .borders(Borders::TOP)
            .title(format!(" key ({} bytes) ", self.key_view.data.len()))
            .style(key_style);
        let key_inner = key_block.inner(chunks[1]);
        frame.render_widget(key_block, chunks[1]);
        let key_para = self.key_view.render(key_inner);
        frame.render_widget(key_para, key_inner);

        // Value panel
        let val_style = if self.active_panel == ActivePanel::Value {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let val_block = Block::default()
            .borders(Borders::TOP)
            .title(format!(" value ({} bytes) ", self.value_view.data.len()))
            .style(val_style);
        let val_inner = val_block.inner(chunks[2]);
        frame.render_widget(val_block, chunks[2]);
        let val_para = self.value_view.render(val_inner);
        frame.render_widget(val_para, val_inner);
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
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
        .split(popup_layout[1])[1]
}
