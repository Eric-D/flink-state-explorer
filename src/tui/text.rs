//! Shortening text for display. Everything here cuts on a `char` boundary and measures in
//! terminal columns, per `char` (ratatui measures whole grapheme clusters, so ZWJ / VS16 emoji
//! sequences may be off by a column, and a cut may split such a cluster — never panic, though): slicing a `&str` at a byte count panics as
//! soon as the cut lands inside a multi-byte character (`é`, CJK, emoji), and a byte count is
//! not a width anyway (`é` is 2 bytes and 1 column, `日` is 3 bytes and 2 columns).

use std::borrow::Cow;

use ratatui::text::Span;

/// Width of `s` in terminal columns: the sum of [`char_width`], so that [`truncate_to_width`]
/// and this agree on every prefix.
pub fn width(s: &str) -> usize {
    s.chars().map(char_width).sum()
}

/// Width of one character in terminal columns, as ratatui measures it (`unicode-width`, which
/// ratatui already depends on): 2 for wide CJK and most emoji, 0 for combining marks.
pub fn char_width(c: char) -> usize {
    let mut buf = [0u8; 4];
    Span::raw(&*c.encode_utf8(&mut buf)).width()
}

/// The longest prefix of `s` that fits in `max` columns, cut on a `char` boundary.
pub fn truncate_to_width(s: &str, max: usize) -> &str {
    let mut used = 0;
    for (i, c) in s.char_indices() {
        used += char_width(c);
        if used > max {
            return &s[..i];
        }
    }
    s
}

/// `s` unchanged when it fits in `max` columns, otherwise its longest prefix that leaves room for
/// `ellipsis`, followed by `ellipsis`. When `max` is narrower than `ellipsis` itself, only the
/// ellipsis is left.
pub fn ellipsize<'a>(s: &'a str, max: usize, ellipsis: &str) -> Cow<'a, str> {
    if width(s) <= max {
        return Cow::Borrowed(s);
    }
    let head = truncate_to_width(s, max.saturating_sub(width(ellipsis)));
    Cow::Owned(format!("{}{}", head, ellipsis))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Strings whose byte offsets and column widths both disagree with their char counts.
    fn samples() -> Vec<String> {
        vec![
            "é".repeat(50),                              // 2 bytes, 1 column each
            "日本語のテキスト".repeat(12),               // 3 bytes, 2 columns each
            "aé日🎉".repeat(20),                         // 1/2/3/4 bytes, 1/1/2/2 columns
            "e\u{301}".repeat(30),                       // e + combining acute: 0-width marks
            "label: ".to_string() + &"Ωμέγα".repeat(25), // ASCII prefix, then Greek
            String::new(),
        ]
    }

    #[test]
    fn truncation_never_panics_and_fits_at_every_width() {
        for s in samples() {
            let full = width(&s);
            for max in 0..=full + 3 {
                let head = truncate_to_width(&s, max);
                assert!(s.starts_with(head));
                assert!(width(head) <= max, "{:?} at {}: {:?}", s, max, head);
                // Longest such prefix: the next character would not have fitted.
                if let Some(next) = s[head.len()..].chars().next() {
                    assert!(width(head) + char_width(next) > max, "{:?} at {}", s, max);
                } else {
                    assert_eq!(head, s);
                }
            }
        }
    }

    #[test]
    fn ellipsize_never_panics_and_respects_the_width() {
        for s in samples() {
            for ellipsis in ["...", "...}", "…"] {
                for max in 0..=width(&s) + 3 {
                    let out = ellipsize(&s, max, ellipsis);
                    if width(&s) <= max {
                        assert_eq!(out, s.as_str());
                    } else {
                        assert!(out.ends_with(ellipsis));
                        let head = &out[..out.len() - ellipsis.len()];
                        assert!(s.starts_with(head));
                        assert!(
                            width(&out) <= max.max(width(ellipsis)),
                            "{:?} at {}",
                            out,
                            max
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn widths_are_columns_not_bytes() {
        assert_eq!(width("abc"), 3);
        assert_eq!(width("é"), 1);
        assert_eq!(width("日本"), 4);
        assert_eq!(width("e\u{301}"), 1);
        assert_eq!(truncate_to_width("日本語", 3), "日");
        assert_eq!(
            ellipsize(&"é".repeat(50), 10, "..."),
            format!("{}...", "é".repeat(7))
        );
    }
}
