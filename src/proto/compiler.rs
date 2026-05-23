use std::path::Path;

use prost_reflect::DescriptorPool;

use crate::error::ProtoError;

/// Compile all `.proto` files found in the given directories into a
/// `DescriptorPool` for dynamic message decoding.
/// Files with missing imports are skipped (warning printed to stderr).
/// Warnings from the last compilation (skipped files).
static COMPILE_WARNINGS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Get warnings from the last compilation.
pub fn last_compile_warnings() -> Vec<String> {
    COMPILE_WARNINGS
        .lock()
        .map(|w| w.clone())
        .unwrap_or_default()
}

pub fn compile_protos(dirs: &[&Path]) -> Result<DescriptorPool, ProtoError> {
    let include_paths: Vec<&str> = dirs.iter().filter_map(|d| d.to_str()).collect();

    let mut compiler =
        protox::Compiler::new(include_paths).map_err(|e| ProtoError::Compile(e.to_string()))?;
    compiler.include_imports(true);

    let mut warnings = Vec::new();
    for dir in dirs {
        // Collect all .proto files recursively
        let proto_files = collect_proto_files(dir);
        for proto_path in &proto_files {
            // open_file needs the path relative to the include dir
            if let Ok(rel) = proto_path.strip_prefix(dir) {
                let rel_str = rel.to_string_lossy().replace('\\', "/");
                if let Err(e) = compiler.open_file(&rel_str) {
                    warnings.push(format!("⚠ {}: {}", rel_str, e));
                }
            }
        }
    }
    if let Ok(mut w) = COMPILE_WARNINGS.lock() {
        *w = warnings;
    }

    let file_descriptor_set = compiler.file_descriptor_set();

    let pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .map_err(|e| ProtoError::Compile(e.to_string()))?;

    Ok(pool)
}

/// Recursively collect all .proto files in a directory.
pub fn list_proto_files(dir: &Path) -> Vec<std::path::PathBuf> {
    collect_proto_files(dir)
}

fn collect_proto_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(collect_proto_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "proto") {
                files.push(path);
            }
        }
    }
    files
}

/// List all message type names available in the descriptor pool.
pub fn list_message_types(pool: &DescriptorPool) -> Vec<String> {
    pool.all_messages()
        .map(|m| m.full_name().to_string())
        .collect()
}
