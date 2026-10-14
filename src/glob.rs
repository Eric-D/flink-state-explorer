//! Key matching shared by the TUI's filter bar and the web UI's search: a case-insensitive glob
//! (`*` any run of characters, `?` one character) and a case-insensitive substring. Pure: no UI
//! crate here, so `--no-default-features` and the web build share it as is.

/// `*` matches any sequence of characters (including none), `?` exactly one; everything else
/// matches itself, ignoring case. Without a wildcard it is an exact (case-insensitive) match.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.to_lowercase().chars().collect();
    let text: Vec<char> = text.to_lowercase().chars().collect();
    glob_chars(&pattern, &text)
}

/// The matcher on lowercased characters: linear backtracking on the last `*` only.
fn glob_chars(pattern: &[char], text: &[char]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == '*' {
            // Star: remember where, try matching zero characters first.
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if star_pi != usize::MAX {
            // Backtrack: the star takes one more character.
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// How a search compares a rendered key with its query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchMode {
    /// [`glob_match`].
    Glob,
    /// The query appears in the key, ignoring case; `*` and `?` are literal.
    Substring,
    /// The key is exactly the query (case and all): a scan for a key `get` cannot locate.
    Exact,
}

impl MatchMode {
    pub fn parse(s: &str) -> Option<MatchMode> {
        match s {
            "glob" => Some(MatchMode::Glob),
            "substring" => Some(MatchMode::Substring),
            "exact" => Some(MatchMode::Exact),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            MatchMode::Glob => "glob",
            MatchMode::Substring => "substring",
            MatchMode::Exact => "exact",
        }
    }
}

/// A query prepared once (lowercased) and tested on many keys.
#[derive(Debug, Clone)]
pub struct KeyMatcher {
    mode: MatchMode,
    query: String,
    lowered: Vec<char>,
}

impl KeyMatcher {
    pub fn new(mode: MatchMode, query: &str) -> Self {
        KeyMatcher {
            mode,
            query: query.to_string(),
            lowered: query.to_lowercase().chars().collect(),
        }
    }

    pub fn matches(&self, key: &str) -> bool {
        match self.mode {
            MatchMode::Exact => key == self.query,
            MatchMode::Glob => {
                let text: Vec<char> = key.to_lowercase().chars().collect();
                glob_chars(&self.lowered, &text)
            }
            MatchMode::Substring => {
                if self.lowered.is_empty() {
                    return true;
                }
                let text: Vec<char> = key.to_lowercase().chars().collect();
                text.windows(self.lowered.len()).any(|w| w == self.lowered)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob() {
        assert!(glob_match("TST*", "tst-1"));
        assert!(glob_match("*inc*", "abc-INCident"));
        assert!(glob_match("a?c", "abc"));
        assert!(glob_match("a?c", "aéc"), "? is one character, not one byte");
        assert!(!glob_match("a?c", "abbc"));
        assert!(glob_match("", ""));
        assert!(!glob_match("", "x"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn modes() {
        let sub = KeyMatcher::new(MatchMode::Substring, "K-1*");
        assert!(sub.matches("xk-1*y"));
        assert!(!sub.matches("k-10"), "* is literal in a substring");
        let exact = KeyMatcher::new(MatchMode::Exact, "Key");
        assert!(exact.matches("Key"));
        assert!(!exact.matches("key"));
        let g = KeyMatcher::new(MatchMode::Glob, "K-1*");
        assert!(g.matches("k-10"));
        assert_eq!(MatchMode::parse("glob"), Some(MatchMode::Glob));
        assert_eq!(MatchMode::parse("x"), None);
    }
}
