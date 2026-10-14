use std::io::Stdout;
use std::path::Path;
use std::time::Instant;

use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Terminal;

use crate::error::{AppError, UiError};
use crate::tui::text;

/// What the TUI does before its first frame: `_metadata`, the profile, then each operator's
/// operator states and keyed-stream headers. No key is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadingStep {
    Start,
    CheckingMetadata,
    ParsingMetadata,
    LoadingProtos,
    ReadingHeaders {
        total_operators: usize,
        current_operator: usize,
        operator_name: String,
    },
    Done,
}

impl LoadingStep {
    fn order(&self) -> usize {
        match self {
            Self::Start => 0,
            Self::CheckingMetadata => 1,
            Self::ParsingMetadata => 2,
            Self::LoadingProtos => 3,
            Self::ReadingHeaders { .. } => 4,
            Self::Done => 5,
        }
    }
}

pub struct LoadingScreen {
    dir_name: String,
    current_step: LoadingStep,
    started_at: Instant,
}

impl LoadingScreen {
    pub fn new(savepoint_dir: &Path) -> Self {
        Self {
            dir_name: savepoint_dir
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| savepoint_dir.display().to_string()),
            current_step: LoadingStep::Start,
            started_at: Instant::now(),
        }
    }

    pub fn advance(&mut self, step: LoadingStep) {
        self.current_step = step;
    }

    pub fn render(
        &self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> Result<(), AppError> {
        terminal
            .draw(|frame| self.draw(frame))
            .map_err(UiError::Render)?;

        Ok(())
    }

    /// Draw the loading popup into `frame`, whatever the backend (tests use `TestBackend`).
    pub fn draw(&self, frame: &mut Frame) {
        let current = &self.current_step;
        let elapsed = self.started_at.elapsed();
        let dir_name = &self.dir_name;
        let area = frame.area();

        let popup_width = 70u16.min(area.width.saturating_sub(4));
        let popup_height = 14u16.min(area.height.saturating_sub(2));
        let x = (area.width.saturating_sub(popup_width)) / 2;
        let y = (area.height.saturating_sub(popup_height)) / 2;
        let popup = Rect::new(x, y, popup_width, popup_height);

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" flink-explorer ")
            .border_style(Style::default().fg(Color::Cyan));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);

        let steps: &[(&str, usize)] = &[
            ("Checking metadata", 1),
            ("Parsing metadata", 2),
            ("Loading proto context", 3),
            ("Reading state headers", 4),
        ];

        let current_order = current.order();
        let spinner_frame = (elapsed.as_millis() / 150) % 4;
        let spinner = match spinner_frame {
            0 => "[|]",
            1 => "[/]",
            2 => "[-]",
            _ => "[\\]",
        };

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(Span::styled(
            format!("Loading {}", dir_name),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(""));

        for &(label, step_order) in steps {
            let (icon, style) = if current_order > step_order {
                ("[ok]", Style::default().fg(Color::Green))
            } else if current_order == step_order {
                (spinner, Style::default().fg(Color::Yellow))
            } else {
                ("[ ]", Style::default().fg(Color::DarkGray))
            };

            // Default label
            let detail = if step_order == 4 {
                // Reading headers — show progress
                match current {
                    LoadingStep::ReadingHeaders {
                        total_operators,
                        current_operator,
                        ..
                    } if current_order == 4 => {
                        format!(
                            " {} — {}/{} operators",
                            label, current_operator, total_operators
                        )
                    }
                    _ if current_order > 4 => format!(" {}", label),
                    _ => format!(" {}", label),
                }
            } else {
                format!(" {}", label)
            };

            lines.push(Line::from(vec![
                Span::styled(format!("{} ", icon), style),
                Span::styled(detail, style),
            ]));
        }

        // Show the current operator's name while its headers are read
        if let LoadingStep::ReadingHeaders { operator_name, .. } = current {
            if !operator_name.is_empty() {
                // Cut by columns on a char boundary: a byte slice panicked on non-ASCII names.
                let name = text::ellipsize(operator_name, 55, "...").into_owned();
                lines.push(Line::from(vec![
                    Span::styled("     ", Style::default()),
                    Span::styled(
                        name,
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::ITALIC),
                    ),
                ]));
            }
        }

        if *current == LoadingStep::Done {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "Ready!",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )));
        }

        let paragraph = Paragraph::new(lines);
        frame.render_widget(paragraph, inner);
    }
}
