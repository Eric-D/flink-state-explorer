pub mod compiler;
pub mod decoder;

use prost_reflect::DescriptorPool;
use std::collections::HashMap;

use crate::profile::storage::ProtoPattern;

/// Context for decoding protobuf bytes found in POJOs.
pub struct ProtoContext {
    pub pool: DescriptorPool,
    pub bindings: HashMap<String, String>,
    pub patterns: Vec<ProtoPattern>,
}

impl ProtoContext {
    /// Check if a POJO matches any configured proto pattern.
    /// Returns (class_field, bytes_field, resolve_strategy) if matched.
    pub fn match_pattern(&self, field_names: &[&str]) -> Option<&ProtoPattern> {
        self.patterns.iter().find(|p| {
            field_names.contains(&p.class_field.as_str())
                && field_names.contains(&p.bytes_field.as_str())
        })
    }

    /// Resolve a className to a proto FQN and decode bytes.
    /// `field_path` is the breadcrumb path for path-based overrides.
    pub fn decode(
        &self,
        class_name: &str,
        bytes: &[u8],
        resolve: &str,
        field_path: Option<&str>,
    ) -> Option<String> {
        // 0. Path-based override (most specific)
        if let Some(path) = field_path {
            for pattern in &self.patterns {
                for (pat, proto_type) in &pattern.path_overrides {
                    if path_matches(path, pat) {
                        if let Ok(decoded) = decoder::decode_message(&self.pool, proto_type, bytes)
                        {
                            return Some(decoded);
                        }
                    }
                }
            }
        }
        // 1. Explicit binding by className
        if let Some(proto_type) = self.bindings.get(class_name) {
            if let Ok(decoded) = decoder::decode_message(&self.pool, proto_type, bytes) {
                return Some(decoded);
            }
        }
        // 2. Strategy-based resolution
        let fqn = match resolve {
            "auto" => resolve_auto(class_name),
            "direct" => Some(class_name.to_string()),
            _ => resolve_auto(class_name),
        };
        if let Some(ref fqn) = fqn {
            if let Ok(decoded) = decoder::decode_message(&self.pool, fqn, bytes) {
                return Some(decoded);
            }
        }
        None
    }
}

/// Match a path against a glob pattern.
/// `*` matches any single segment, `**` matches any number of segments.
/// Segments are separated by `.` or ` > `.
fn path_matches(path: &str, pattern: &str) -> bool {
    // Normalize separators
    let path_parts: Vec<&str> = path
        .split(|c| c == '.' || c == '>')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let pat_parts: Vec<&str> = pattern
        .split(|c| c == '.' || c == '>')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    // Simple glob: match from the end (pattern is typically a suffix)
    if pat_parts.first() == Some(&"*") {
        // "*" at start = match any prefix, check suffix
        let suffix = &pat_parts[1..];
        if suffix.len() > path_parts.len() {
            return false;
        }
        let path_suffix = &path_parts[path_parts.len() - suffix.len()..];
        return suffix
            .iter()
            .zip(path_suffix.iter())
            .all(|(p, s)| *p == "*" || p == s);
    }

    // Exact match
    if pat_parts.len() != path_parts.len() {
        return false;
    }
    pat_parts
        .iter()
        .zip(path_parts.iter())
        .all(|(p, s)| *p == "*" || p == s)
}

pub fn resolve_auto_pub(class_name: &str) -> Option<String> {
    resolve_auto(class_name)
}

/// Auto-resolve: `fr.asse.proto.Effectif$FicheJoueur` → `fr.asse.proto.FicheJoueur`
fn resolve_auto(class_name: &str) -> Option<String> {
    let dollar_pos = class_name.rfind('$')?;
    let dot_pos = class_name[..dollar_pos].rfind('.')?;
    let package = &class_name[..dot_pos];
    let message = &class_name[dollar_pos + 1..];
    Some(format!("{}.{}", package, message))
}
