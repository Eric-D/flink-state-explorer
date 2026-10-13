//! Headless benchmark of the explorer's TUI load path. Not part of the product.
//!
//! The TUI cannot run without a terminal, so this drives the same functions `App::new_with_progress`
//! calls: `parse_metadata` → `build_index_with_progress` → `save_cache` → `TreeView::new`, then
//! draws frames into a `ratatui::backend::TestBackend` (200x50) and walks the tree through the
//! public `jump_first`/`move_down`/`expand` API. Wall time and RSS/HWM (from `/proc/self/status`)
//! are printed after each step.
//!
//! ```text
//! cargo run --release --example bench_index -- <savepoint dir> [build|load|scroll]
//! ```
//! - `build` (default): removes `.flink-explorer-cache`, builds the index, saves the cache, builds
//!   the tree and renders — the TUI's cold start.
//! - `load`: loads the cache written by `build`, builds the tree and renders — the warm start.
//! - `scroll`: expands the biggest operator and scrolls past the stored partitions.

use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use flink_explorer::index::builder::build_index_with_progress;
use flink_explorer::index::cache::{load_cache, save_cache, IndexCache};
use flink_explorer::parser::parse_metadata;
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::widgets::Block;
use ratatui::Terminal;

const CACHE_FILE: &str = ".flink-explorer-cache";
const IDLE_FRAMES: usize = 200;

/// `(VmRSS, VmHWM)` in MiB, read from `/proc/self/status`.
fn rss_mb() -> (f64, f64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let grab = |key: &str| {
        status
            .lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .map_or(0.0, |kb| kb / 1024.0)
    };
    (grab("VmRSS:"), grab("VmHWM:"))
}

fn report(step: &str, elapsed: Duration, extra: &str) {
    let (rss, hwm) = rss_mb();
    println!(
        "{:<40} {:>9.3} s   RSS {:>8.1} MB   HWM {:>8.1} MB   {}",
        step,
        elapsed.as_secs_f64(),
        rss,
        hwm,
        extra
    );
}

/// Same `(size, mtime)` pair the TUI stamps the cache with (`app.rs::get_file_stats`).
fn file_stats(path: &Path) -> (u64, u64) {
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_secs());
            (meta.len(), mtime)
        }
        Err(_) => (0, 0),
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .expect("usage: bench_index <savepoint dir> [build|load|scroll]"),
    );
    let mode = args.next().unwrap_or_else(|| "build".to_string());
    let metadata_path = dir.join("_metadata");
    let (size, mtime) = file_stats(&metadata_path);
    println!("fixture {}  mode {}", dir.display(), mode);
    report("process start", Duration::ZERO, "");
    match mode.as_str() {
        "build" => run_build(&dir, &metadata_path, size, mtime),
        "load" => run_load(&dir, size, mtime),
        "scroll" => run_scroll(&dir, size, mtime),
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }
}

/// Cold start: no cache, full index build, cache written, tree built, frames drawn.
fn run_build(dir: &Path, metadata_path: &Path, size: u64, mtime: u64) {
    let cache_path = dir.join(CACHE_FILE);
    let _ = std::fs::remove_file(&cache_path);

    let t = Instant::now();
    let savepoint = parse_metadata(metadata_path).expect("parse _metadata");
    report(
        "parse_metadata",
        t.elapsed(),
        &format!("{} operators", savepoint.operators.len()),
    );

    let mut progress_calls = 0usize;
    let t = Instant::now();
    let index = build_index_with_progress(&savepoint, size, mtime, dir, |_, _, _| {
        progress_calls += 1;
    });
    report(
        "build_index_with_progress",
        t.elapsed(),
        &format!("{} progress callbacks", progress_calls),
    );
    print_key_counts(&index);

    let t = Instant::now();
    save_cache(&index, dir).expect("save_cache");
    let bytes = std::fs::metadata(&cache_path).map_or(0, |m| m.len());
    report(
        "save_cache",
        t.elapsed(),
        &format!("{} bytes ({:.1} MB)", bytes, bytes as f64 / 1_048_576.0),
    );

    // `App` owns both the index and the tree, so the index is deliberately kept alive here.
    let t = Instant::now();
    let mut tree = TreeView::new(&index);
    report("TreeView::new", t.elapsed(), "");
    frame_bench(&mut tree, &index);
}

/// Warm start: cache hit, tree built, frames drawn.
fn run_load(dir: &Path, size: u64, mtime: u64) {
    let t = Instant::now();
    let index = load_cache(dir, size, mtime).expect("load_cache (run the `build` mode first)");
    report(
        "load_cache",
        t.elapsed(),
        &format!("{} operators", index.operators.len()),
    );
    print_key_counts(&index);

    let t = Instant::now();
    let mut tree = TreeView::new(&index);
    report("TreeView::new", t.elapsed(), "");
    frame_bench(&mut tree, &index);
}

fn print_key_counts(index: &IndexCache) {
    for op in &index.operators {
        let Some(ks) = &op.keyed_state else { continue };
        let stored = ks.partitions.len();
        let raw_bytes: usize = ks
            .partitions
            .iter()
            .map(|p| {
                p.key.len()
                    + p.values
                        .iter()
                        .map(|v| v.as_ref().map_or(0, Vec::len))
                        .sum::<usize>()
            })
            .sum();
        println!(
            "  keyed operator {} ({}): total_keys={} stored_partitions={} truncated_by_cap={} \
             descriptors={} stored key+value bytes={}",
            op.operator_id_hex(),
            op.display_name,
            ks.total_keys,
            stored,
            stored < ks.total_keys,
            ks.descriptors.len(),
            raw_bytes
        );
    }
}

fn draw(tree: &mut TreeView, terminal: &mut Terminal<TestBackend>) -> Duration {
    let t = Instant::now();
    terminal
        .draw(|f| tree.render(f, Block::bordered().title("Savepoint"), f.area()))
        .expect("draw");
    t.elapsed()
}

/// Walk the collapsed tree and return `(move_down steps from the top, op_idx, total_keys)` of the
/// operator with the most keys.
fn find_biggest_operator(tree: &mut TreeView, index: &IndexCache) -> Option<(usize, usize, usize)> {
    tree.jump_first();
    let mut best: Option<(usize, usize, usize)> = None;
    let mut steps = 0usize;
    loop {
        if let Some(op) = tree.selected_op_idx() {
            let keys = index.operators[op]
                .keyed_state
                .as_ref()
                .map_or(0, |k| k.total_keys);
            if best.is_none_or(|b| keys > b.2) {
                best = Some((steps, op, keys));
            }
        }
        let before = tree.selected_op_idx();
        tree.move_down();
        steps += 1;
        if tree.selected_op_idx() == before {
            break;
        }
    }
    best
}

fn frame_bench(tree: &mut TreeView, index: &IndexCache) {
    let mut terminal = Terminal::new(TestBackend::new(200, 50)).expect("terminal");

    // `TreeView::new` assumes a 120-column display; the first render at 200 columns triggers a
    // `rebuild_flat_list`, exactly as the real first frame does.
    let d = draw(tree, &mut terminal);
    report("first frame (collapsed tree)", d, "");

    let Some((steps, op_idx, keys)) = find_biggest_operator(tree, index) else {
        println!("no operator visible in the tree");
        return;
    };
    let op = &index.operators[op_idx];
    tree.jump_first();
    for _ in 0..steps {
        tree.move_down();
    }
    let t = Instant::now();
    tree.expand();
    report(
        "expand() biggest operator",
        t.elapsed(),
        &format!("{} ({} keys)", op.display_name, keys),
    );
    let d = draw(tree, &mut terminal);
    report("frame after operator expand", d, "");

    // The event loop redraws every 100 ms when idle; this is what each of those frames costs.
    let mut max = Duration::ZERO;
    let t = Instant::now();
    for _ in 0..IDLE_FRAMES {
        max = max.max(draw(tree, &mut terminal));
    }
    let total = t.elapsed();
    report(
        &format!("{IDLE_FRAMES} idle frames (operator expanded)"),
        total,
        &format!(
            "mean {:.1} us/frame, max {:.1} us",
            total.as_secs_f64() * 1e6 / IDLE_FRAMES as f64,
            max.as_secs_f64() * 1e6
        ),
    );

    // 60 key presses down: the first 30 move within the page, the rest scroll it one key at a
    // time (`scroll_partitions` → `rebuild_flat_list` on every press).
    let mut max = Duration::ZERO;
    let t = Instant::now();
    for _ in 0..60 {
        let t1 = Instant::now();
        tree.move_down();
        max = max.max(t1.elapsed());
    }
    report(
        "60 x move_down (pages keys)",
        t.elapsed(),
        &format!("max {:.1} us/press", max.as_secs_f64() * 1e6),
    );

    // Expand the selected key: its state values are decoded into labels.
    let t = Instant::now();
    tree.expand();
    let elapsed = t.elapsed();
    report(
        "expand() one key (decodes values)",
        elapsed,
        tree.selected_path(),
    );
    let d = draw(tree, &mut terminal);
    report("frame after key expand", d, "");

    // Expand the key's first three states (a MAP explodes into one node per entry).
    for _ in 0..3 {
        tree.move_down();
        tree.expand();
    }
    let d = draw(tree, &mut terminal);
    report("frame after 3 state expands", d, "");

    let buf = terminal.backend().buffer();
    println!(
        "--- rendered buffer, first 14 of {} rows ({} cols) ---",
        buf.area.height, buf.area.width
    );
    for y in 0..buf.area.height.min(14) {
        let line: String = (0..buf.area.width)
            .map(|x| buf.cell((x, y)).map_or(" ", |c| c.symbol()))
            .collect();
        println!("{}", line.trim_end());
    }
}

/// Expand the biggest operator and press Down until the page runs past the partitions the index
/// actually stores (`MAX_STORED_PARTITIONS`), reporting whether the TUI survives.
fn run_scroll(dir: &Path, size: u64, mtime: u64) {
    let index = load_cache(dir, size, mtime).expect("load_cache (run the `build` mode first)");
    let mut tree = TreeView::new(&index);
    let mut terminal = Terminal::new(TestBackend::new(200, 50)).expect("terminal");
    draw(&mut tree, &mut terminal);
    let Some((steps, op_idx, total)) = find_biggest_operator(&mut tree, &index) else {
        println!("no operator visible in the tree");
        return;
    };
    let stored = index.operators[op_idx]
        .keyed_state
        .as_ref()
        .map_or(0, |k| k.partitions.len());
    tree.jump_first();
    for _ in 0..steps {
        tree.move_down();
    }
    tree.expand();
    let presses = stored + 40;
    println!(
        "operator {} total_keys={} stored_partitions={}; pressing Down {} times",
        index.operators[op_idx].display_name, total, stored, presses
    );
    let t = Instant::now();
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
        for i in 0..presses {
            tree.move_down();
            if i % 25_000 == 0 && i > 0 {
                println!("  {} presses, now at {:?}", i, tree.selected_path());
            }
        }
    }));
    match outcome {
        Ok(()) => report(
            "scroll past stored partitions",
            t.elapsed(),
            tree.selected_path(),
        ),
        Err(e) => {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic payload".to_string());
            report(
                "scroll past stored partitions",
                t.elapsed(),
                &format!("PANIC: {msg}"),
            );
        }
    }
    let d = draw(&mut tree, &mut terminal);
    report("frame after scroll", d, "");
}
