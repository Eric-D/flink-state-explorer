//! HTML whose escaping is a type invariant.
//!
//! An [`Html`] can only grow by [`Html::text`] (escaped) or [`Html::raw`] (a `&'static str`, so
//! only a template written in this source tree, never data read from a savepoint). There is no
//! way to append a runtime `String` unescaped: keys, values, state names, paths and error
//! messages all go through [`escape`].

use std::fmt::Write;

/// A fragment of HTML. See the [module documentation](self).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Html(String);

impl Html {
    pub fn new() -> Self {
        Html(String::new())
    }

    /// Append template markup. `'static` on purpose: a string built at runtime cannot be passed.
    pub fn raw(&mut self, markup: &'static str) -> &mut Self {
        self.0.push_str(markup);
        self
    }

    /// Append text, escaped for both element content and quoted attribute values.
    pub fn text(&mut self, text: &str) -> &mut Self {
        escape_into(&mut self.0, text);
        self
    }

    /// Append anything `Display`, escaped (numbers, cursors, …).
    pub fn display(&mut self, v: impl std::fmt::Display) -> &mut Self {
        let mut s = String::new();
        let _ = write!(s, "{}", v);
        self.text(&s)
    }

    /// Append another fragment (itself built only from escaped text and templates).
    pub fn append(&mut self, other: &Html) -> &mut Self {
        self.0.push_str(&other.0);
        self
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Escape `&`, `<`, `>`, `"` and `'`: safe in element content and in a quoted attribute value.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    escape_into(&mut out, text);
    out
}

fn escape_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
}

/// Percent-encode a URL component (path segment or query value): everything but the RFC 3986
/// unreserved characters. The result is still passed through [`Html::text`] when written into an
/// attribute.
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => {
                let _ = write!(out, "%{:02X}", b);
            }
        }
    }
    out
}

/// Decode a percent-encoded query component (`+` is a space, as a form submits it). Invalid
/// escapes are kept as they are; invalid UTF-8 is replaced.
pub fn url_decode(s: &str) -> String {
    percent_decode(s, true)
}

/// Decode a percent-encoded **path segment**: like [`url_decode`], but `+` stays a `+` (only a
/// query string uses it for a space).
pub fn url_decode_path(s: &str) -> String {
    percent_decode(s, false)
}

fn percent_decode(s: &str, plus_is_space: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' if plus_is_space => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = |c: u8| (c as char).to_digit(16);
                match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 3;
                        continue;
                    }
                    _ => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_is_escaped() {
        let mut h = Html::new();
        h.raw("<p title=\"")
            .text("\"><script>x</script>'")
            .raw("\">");
        assert_eq!(
            h.as_str(),
            "<p title=\"&quot;&gt;&lt;script&gt;x&lt;/script&gt;&#39;\">"
        );
    }

    #[test]
    fn url_round_trip() {
        for s in ["a b/c?d=e&f", "_timer_state/x", "é<>", "%zz", "100%"] {
            assert_eq!(url_decode(&url_encode(s)), s);
        }
        assert_eq!(url_decode("a+b%2Fc"), "a b/c");
        assert_eq!(url_decode("%zz%4"), "%zz%4");
        assert_eq!(url_decode_path("a+b%2Bc%20d"), "a+b+c d");
    }
}
