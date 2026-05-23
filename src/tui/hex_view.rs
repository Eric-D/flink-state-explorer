use ratatui::prelude::*;
use ratatui::widgets::Paragraph;

const BYTES_PER_LINE: usize = 16;

pub struct HexView {
    pub data: Vec<u8>,
    pub scroll_offset: usize,
    pub label: String,
}

impl HexView {
    pub fn new(data: Vec<u8>, label: &str) -> Self {
        Self {
            data,
            scroll_offset: 0,
            label: label.to_string(),
        }
    }

    pub fn total_lines(&self) -> usize {
        self.data.len().div_ceil(BYTES_PER_LINE)
    }

    pub fn scroll_up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    pub fn scroll_down(&mut self) {
        let max = self.total_lines().saturating_sub(1);
        if self.scroll_offset < max {
            self.scroll_offset += 1;
        }
    }

    pub fn render(&self, area: Rect) -> Paragraph<'_> {
        let visible_lines = area.height as usize;
        let mut lines: Vec<Line> = Vec::with_capacity(visible_lines);

        for line_idx in 0..visible_lines {
            let data_line = self.scroll_offset + line_idx;
            let start = data_line * BYTES_PER_LINE;
            if start >= self.data.len() {
                lines.push(Line::from("~"));
                continue;
            }
            let end = (start + BYTES_PER_LINE).min(self.data.len());
            let chunk = &self.data[start..end];

            // Offset
            let offset_str = format!("{:08x}  ", start);

            // Hex bytes
            let mut hex_parts = String::with_capacity(BYTES_PER_LINE * 3 + 1);
            for (i, byte) in chunk.iter().enumerate() {
                if i == 8 {
                    hex_parts.push(' ');
                }
                hex_parts.push_str(&format!("{:02x} ", byte));
            }
            // Pad if short line
            for i in chunk.len()..BYTES_PER_LINE {
                if i == 8 {
                    hex_parts.push(' ');
                }
                hex_parts.push_str("   ");
            }

            // ASCII
            let ascii: String = chunk
                .iter()
                .map(|&b| {
                    if b.is_ascii_graphic() || b == b' ' {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();

            let line = format!("{}{} |{}|", offset_str, hex_parts, ascii);
            lines.push(Line::from(line));
        }

        Paragraph::new(lines).style(Style::default().fg(Color::White))
    }
}
