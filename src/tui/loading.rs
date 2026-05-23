use std::io::Stdout;
use std::path::Path;
use std::time::Instant;

use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Terminal;

use crate::error::{AppError, UiError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadingStep {
    Start,
    CheckingMetadata,
    LoadingCache,
    ParsingMetadata,
    BuildingIndex {
        total_operators: usize,
        current_operator: usize,
        operator_name: String,
        total_keys: usize,
    },
    BuildingTree,
    LoadingProtos,
    Done,
}

impl LoadingStep {
    fn order(&self) -> usize {
        match self {
            Self::Start => 0,
            Self::CheckingMetadata => 1,
            Self::LoadingCache => 2,
            Self::ParsingMetadata => 3,
            Self::BuildingIndex { .. } => 4,
            Self::BuildingTree => 5,
            Self::LoadingProtos => 6,
            Self::Done => 7,
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
        let current = &self.current_step;
        let elapsed = self.started_at.elapsed();
        let dir_name = &self.dir_name;

        terminal
            .draw(|frame| {
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
                    ("Loading cache", 2),
                    ("Parsing metadata", 3),
                    ("Building index", 4),
                    ("Building tree", 5),
                    ("Loading proto context", 6),
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
                        // Building index — show progress
                        match current {
                            LoadingStep::BuildingIndex {
                                total_operators,
                                current_operator,
                                total_keys,
                                ..
                            } if current_order == 4 => {
                                format!(
                                    " {} — {}/{} operators, {} keys",
                                    label, current_operator, total_operators, total_keys
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

                // Show current operator name during index building
                if let LoadingStep::BuildingIndex { operator_name, .. } = current {
                    if !operator_name.is_empty() {
                        let name = if operator_name.len() > 55 {
                            format!("{}...", &operator_name[..52])
                        } else {
                            operator_name.clone()
                        };
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
            })
            .map_err(UiError::Render)?;

        Ok(())
    }
}
