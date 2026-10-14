//! Headless benchmark of the TUI's load and browse path, on the `StateReader`. Not part of the
//! product.
//!
//! The TUI cannot run without a terminal, so this drives what `flink-explorer` does through the
//! public API: `App::open` (parse `_metadata`, read operator states and keyed headers — the work
//! before the first frame), a first frame drawn into a `ratatui::backend::TestBackend` (200x50),
//! then expanding the operator with the most keyed state and paging through `N` keys with
//! `PageDown`, the background count pass, and a glob search. Wall time and RSS/HWM (from
//! `/proc/self/status`, Linux) are printed after each step. Nothing is written anywhere (no
//! counts cache).
//!
//! ```text
//! cargo run --release --example bench_tui -- <savepoint dir> [keys to page (default 100000)] [glob]
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use flink_explorer::tui::app::{App, AppOptions};
use flink_explorer::tui::filter::AppliedFilter;
use flink_explorer::tui::tree::KeyCount;
use ratatui::backend::TestBackend;
use ratatui::widgets::{Block, Borders};
use ratatui::Terminal;

/// `(VmRSS, VmHWM)` in MiB, read from `/proc/self/status` (0 elsewhere).
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
        "{:<36} {:>9.3} s   RSS {:>7.1} MB   HWM {:>7.1} MB   {}",
        step,
        elapsed.as_secs_f64(),
        rss,
        hwm,
        extra
    );
}

/// The selected row's last path element (a key, when on a key row).
fn selected_key(app: &mut App) -> String {
    app.tree()
        .selected_path()
        .rsplit(" > ")
        .next()
        .unwrap_or_default()
        .to_string()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .expect("usage: bench_tui <savepoint dir> [keys] [glob]"),
    );
    let keys_to_page: usize = args
        .next()
        .map_or(100_000, |n| n.parse().expect("a number"));
    let glob = args.next().unwrap_or_else(|| "*99999*".to_string());

    let started = Instant::now();
    let mut app = App::open(
        dir.clone(),
        AppOptions {
            cache_dir: None,
            count_keys: false,
            count_step_bytes: 0,
        },
        |_| {},
    )
    .expect("open savepoint");
    report("open (_metadata + headers)", started.elapsed(), "");

    let mut terminal = Terminal::new(TestBackend::new(200, 50)).unwrap();
    terminal.draw(|f| app.draw(f)).unwrap();
    report("first frame", started.elapsed(), "time to first frame");

    // The operator with the most keyed state.
    let biggest = {
        let tree = app.tree();
        (0..tree.operator_count())
            .filter(|&i| tree.key_count(i).is_some())
            .max_by_key(|&i| tree.summary(i).map_or(0, |s| s.keyed_state_size))
    };
    let Some(op) = biggest else {
        println!("no keyed operator");
        return;
    };
    let name = app.tree().summary(op).unwrap().display_name.clone();
    let t = Instant::now();
    {
        let tree = app.tree();
        tree.jump_first();
        while tree.selected_op_idx() != Some(op) || tree.selected_path() != name {
            tree.move_down();
        }
        tree.expand();
    }
    terminal.draw(|f| app.draw(f)).unwrap();
    report("expand + first page", t.elapsed(), &name);

    // Page through N keys: each PageDown scrolls a screen of keys (~48).
    let t = Instant::now();
    let mut presses = 0usize;
    let mut last = String::new();
    let mut frame_times = Vec::new();
    while app.tree().decode_calls() < keys_to_page {
        app.tree().page_down();
        presses += 1;
        let f = Instant::now();
        terminal.draw(|fr| app.draw(fr)).unwrap();
        frame_times.push(f.elapsed());
        let key = selected_key(&mut app);
        if key == last {
            break; // the end
        }
        last = key;
    }
    let paged = app.tree().decode_calls();
    frame_times.sort();
    let p = |q: f64| frame_times[((frame_times.len() - 1) as f64 * q) as usize];
    report(
        &format!("page {} keys", paged),
        t.elapsed(),
        &format!(
            "{} PageDown, {:.2} ms/press, frame p50 {:.2} ms p99 {:.2} ms",
            presses,
            t.elapsed().as_secs_f64() * 1000.0 / presses.max(1) as f64,
            p(0.5).as_secs_f64() * 1000.0,
            p(0.99).as_secs_f64() * 1000.0
        ),
    );

    // The background count pass, to its end.
    let t = Instant::now();
    app.start_count();
    app.wait_for_count();
    let count = app.tree().key_count(op);
    report(
        "count pass",
        t.elapsed(),
        &match count {
            Some(KeyCount::Exact(n)) => format!("{} keys", n),
            other => format!("{:?}", other),
        },
    );

    // A glob over every key.
    let t = Instant::now();
    let tree = app.tree();
    tree.apply_filter(&Some(AppliedFilter::Pattern(glob.clone())));
    let mut steps = 0;
    while tree.search_step(flink_explorer::tui::tree::SEARCH_STEP_BYTES) {
        steps += 1;
    }
    let mut terminal = Terminal::new(TestBackend::new(200, 50)).unwrap();
    terminal
        .draw(|f| tree.render(f, Block::default().borders(Borders::ALL), f.area()))
        .unwrap();
    let buf = terminal.backend().buffer();
    let first_row: String = (0..buf.area.width).map(|x| buf[(x, 1)].symbol()).collect();
    report(
        &format!("search {}", glob),
        t.elapsed(),
        &format!("{} steps; {}", steps + 1, first_row.trim()),
    );
    report("total", started.elapsed(), "");
}
