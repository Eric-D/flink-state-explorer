use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::ProfileError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub name: String,
    #[serde(default)]
    pub proto_dirs: Vec<PathBuf>,
    #[serde(default)]
    pub bindings: HashMap<String, String>,
    #[serde(default)]
    pub proto_patterns: Vec<ProtoPattern>,
}

/// Configurable pattern for detecting protobuf-serialized fields in POJOs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoPattern {
    /// Field name containing the proto class/type name (e.g., "className")
    pub class_field: String,
    /// Field name containing the raw protobuf bytes (e.g., "protoBytes")
    pub bytes_field: String,
    /// Resolution strategy: "auto" (package.Outer$Message → package.Message)
    /// or "direct" (className used as-is for proto lookup)
    #[serde(default = "default_resolve")]
    pub resolve: String,
    /// Path-based overrides: path pattern → proto message FQN.
    /// Path uses the breadcrumb format shown in the status bar.
    /// Supports glob-like `*` for any segment.
    /// Example: "*.ficheJoueur.content.protoBytes" = "...FicheJoueur"
    #[serde(default)]
    pub path_overrides: HashMap<String, String>,
}

fn default_resolve() -> String {
    "auto".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalConfig {
    pub last_profile: Option<String>,
}

/// Binary directory (where the executable lives).
pub fn binary_dir() -> PathBuf {
    bin_dir()
}

fn bin_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn cwd() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Config directories in priority order (CWD overrides binary dir).
pub fn config_dirs() -> Vec<PathBuf> {
    let bin = bin_dir().join("config");
    let local = cwd().join("config");
    let mut dirs = vec![bin];
    // Only add CWD if it's different from binary dir
    if local != dirs[0] && local.exists() {
        dirs.push(local);
    }
    dirs
}

fn config_dir() -> PathBuf {
    bin_dir().join("config")
}

fn profiles_dir() -> PathBuf {
    config_dir().join("profiles")
}

fn profile_path(name: &str) -> PathBuf {
    profiles_dir().join(format!("{}.toml", name))
}

/// Return the path where this profile is found (CWD layer first, then binary dir).
pub fn profile_file_path(name: &str) -> PathBuf {
    for dir in config_dirs().iter().rev() {
        let path = dir.join("profiles").join(format!("{}.toml", name));
        if path.exists() {
            return path;
        }
    }
    profile_path(name) // fallback to binary dir
}

fn global_config_path() -> PathBuf {
    config_dir().join("config.toml")
}

/// Load config by merging `<binary_dir>/config/config.toml` with
/// `<cwd>/config/config.toml` (CWD overrides/extends binary dir).
pub fn load_config_as_profile() -> Result<Profile, ProfileError> {
    let dirs = config_dirs();
    let mut base: Option<Profile> = None;

    for dir in &dirs {
        let path = dir.join("config.toml");
        if path.exists() {
            let content = std::fs::read_to_string(&path).map_err(ProfileError::Read)?;
            let layer: Profile = toml::from_str(&content)?;
            base = Some(match base {
                Some(b) => merge_profiles(b, layer),
                None => layer,
            });
        }
    }

    base.ok_or_else(|| ProfileError::NotFound("config.toml".to_string()))
}

/// Load a named profile, merging from binary dir and CWD layers.
pub fn load_profile(name: &str) -> Result<Profile, ProfileError> {
    let dirs = config_dirs();
    let mut base: Option<Profile> = None;

    for dir in &dirs {
        let path = dir.join("profiles").join(format!("{}.toml", name));
        if path.exists() {
            let content = std::fs::read_to_string(&path).map_err(ProfileError::Read)?;
            let layer: Profile = toml::from_str(&content)?;
            base = Some(match base {
                Some(b) => merge_profiles(b, layer),
                None => layer,
            });
        }
    }

    base.ok_or_else(|| ProfileError::NotFound(name.to_string()))
}

/// Merge two profiles: `overlay` extends/overrides `base`.
fn merge_profiles(base: Profile, overlay: Profile) -> Profile {
    let mut proto_dirs = base.proto_dirs;
    for d in overlay.proto_dirs {
        if !proto_dirs.contains(&d) {
            proto_dirs.push(d);
        }
    }
    let mut bindings = base.bindings;
    bindings.extend(overlay.bindings);
    let mut proto_patterns = base.proto_patterns;
    proto_patterns.extend(overlay.proto_patterns);

    Profile {
        name: if overlay.name.is_empty() {
            base.name
        } else {
            overlay.name
        },
        proto_dirs,
        bindings,
        proto_patterns,
    }
}

pub fn save_profile(profile: &Profile) -> Result<(), ProfileError> {
    let dir = profiles_dir();
    std::fs::create_dir_all(&dir).map_err(ProfileError::Write)?;
    let content = toml::to_string_pretty(profile)?;
    std::fs::write(profile_path(&profile.name), content).map_err(ProfileError::Write)?;
    Ok(())
}

/// List profiles from all config layers (binary dir + CWD).
pub fn list_profiles() -> Result<Vec<String>, ProfileError> {
    let mut names = std::collections::BTreeSet::new();
    for dir in config_dirs() {
        let profiles = dir.join("profiles");
        if !profiles.exists() {
            continue;
        }
        if let Ok(entries) = std::fs::read_dir(&profiles) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "toml") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        names.insert(stem.to_string());
                    }
                }
            }
        }
    }
    Ok(names.into_iter().collect())
}

pub fn delete_profile(name: &str) -> Result<(), ProfileError> {
    let path = profile_path(name);
    if path.exists() {
        std::fs::remove_file(&path).map_err(ProfileError::Write)?;
    }
    // If this was the last profile, clear config
    if let Ok(mut config) = load_config() {
        if config.last_profile.as_deref() == Some(name) {
            config.last_profile = None;
            let _ = save_config(&config);
        }
    }
    Ok(())
}

pub fn load_config() -> Result<GlobalConfig, ProfileError> {
    let path = global_config_path();
    if !path.exists() {
        return Ok(GlobalConfig::default());
    }
    let content = std::fs::read_to_string(&path).map_err(ProfileError::Read)?;
    let config: GlobalConfig = toml::from_str(&content)?;
    Ok(config)
}

pub fn save_config(config: &GlobalConfig) -> Result<(), ProfileError> {
    let dir = config_dir();
    std::fs::create_dir_all(&dir).map_err(ProfileError::Write)?;
    let content = toml::to_string_pretty(config)?;
    std::fs::write(global_config_path(), content).map_err(ProfileError::Write)?;
    Ok(())
}

/// Set a profile as the last used.
pub fn set_last_profile(name: &str) -> Result<(), ProfileError> {
    let mut config = load_config()?;
    config.last_profile = Some(name.to_string());
    save_config(&config)
}
