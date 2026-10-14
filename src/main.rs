use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use crossterm::event;
use ratatui::prelude::*;
use ratatui::Terminal;

use flink_explorer::error::{AppError, UiError};
use flink_explorer::tui::app::App;
use flink_explorer::tui::loading::{LoadingScreen, LoadingStep};
use flink_explorer::tui::terminal::{restore_terminal, TerminalGuard};

#[derive(Parser)]
#[command(
    name = "flink-explorer",
    version,
    about = "TUI tool for exploring Apache Flink 1.20 savepoints"
)]
struct Cli {
    /// Path to the Flink savepoint directory containing _metadata
    savepoint_dir: PathBuf,

    /// Load a specific profile by name
    #[arg(short, long)]
    profile: Option<String>,

    /// Neither read nor write the key counts cache (under the user cache dir; never in the
    /// savepoint directory): counts are then computed again in the background
    #[arg(long)]
    no_cache: bool,

    /// Export a single keyed state as NDJSON instead of opening the TUI (streaming: memory stays
    /// bounded by the largest key-group, not by the size of the state)
    #[arg(long, value_name = "STATE")]
    export_state: Option<String>,

    /// Restrict --export-state to one operator, matched as a prefix of its hex OperatorID
    #[arg(long, value_name = "HEX")]
    operator: Option<String>,

    /// Where --export-state writes; "-" means stdout (ndjson only)
    #[arg(short, long, value_name = "PATH")]
    out: Option<PathBuf>,

    /// Output format for --export-state. `parquet` needs a build with `--features parquet`.
    #[arg(long, value_name = "FMT", default_value = "ndjson")]
    format: String,

    /// List the keyed states available to --export-state, then exit
    #[arg(long)]
    list_states: bool,

    /// Serve a read-only browser UI instead of the TUI (prints the URL to open, with its token)
    #[arg(long, conflicts_with_all = ["export_state", "list_states"])]
    web: bool,

    /// Port for --web; 0 picks a free one
    #[arg(long, value_name = "N", default_value_t = 0, requires = "web")]
    port: u16,

    /// Address --web binds to. Only loopback is private: anything else exposes the savepoint's
    /// data to the network (the token still guards it)
    #[arg(long, value_name = "IP", default_value = "127.0.0.1", requires = "web")]
    bind: std::net::IpAddr,

    /// With --web, open the URL in the default browser
    #[arg(long, requires = "web")]
    open: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // Headless paths must not touch the terminal: they are meant to be piped and scripted.
    if cli.list_states {
        return match list_states(&cli) {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("Error: {}", e);
                ExitCode::from(2)
            }
        };
    }
    if cli.web {
        return match web(&cli) {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("Error: {}", e);
                ExitCode::from(2)
            }
        };
    }
    if cli.export_state.is_some() {
        return match export_state(&cli) {
            Ok(()) => ExitCode::from(0),
            // `--export-state … -o - | head`: the reader went away, which is not an error.
            Err(e) if is_stdout(&cli) && e.to_lowercase().contains("broken pipe") => {
                ExitCode::from(0)
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                ExitCode::from(2)
            }
        };
    }

    match run(cli) {
        Ok(()) => ExitCode::from(0),
        Err(e) => {
            eprintln!("Error: {}", e);
            match e {
                AppError::Parse(_) => ExitCode::from(2),
                AppError::Ui(_) => ExitCode::from(3),
                _ => ExitCode::from(1),
            }
        }
    }
}

fn run(cli: Cli) -> Result<(), AppError> {
    // Enter TUI immediately so we can show loading progress. The guard installs a panic hook
    // before raw mode is enabled, and restores the terminal however this function is left.
    let _guard = TerminalGuard::enter().map_err(UiError::Init)?;

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).map_err(UiError::Init)?;

    load_and_run(cli, &mut terminal)
}

fn load_and_run(
    cli: Cli,
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
) -> Result<(), AppError> {
    let mut loading = LoadingScreen::new(&cli.savepoint_dir);
    loading.render(terminal)?;

    let mut app = App::new_with_progress(cli.savepoint_dir, cli.no_cache, |step| {
        loading.advance(step);
        let _ = loading.render(terminal);
        // A Ctrl-C between two loading steps (headers are read one operator at a time) ends the
        // process here: raw mode turned it into a key event, so no SIGINT does it for us.
        if take_queued_input() {
            restore_terminal(); // process::exit skips the guard's Drop
            std::process::exit(130);
        }
    })?;

    loading.advance(LoadingStep::Done);
    loading.render(terminal)?;
    if take_queued_input() {
        return Ok(());
    }

    app.run_with_terminal(terminal)
}

/// Read every input event already queued, without blocking, and say whether one was Ctrl-C
/// (the rest are dropped: see `drain_is_interrupt`).
fn take_queued_input() -> bool {
    let queued = std::iter::from_fn(|| match event::poll(Duration::ZERO) {
        Ok(true) => event::read().ok(),
        _ => None,
    });
    flink_explorer::tui::app::drain_is_interrupt(queued)
}

/// Parse `_metadata` alone (the headless paths read state files themselves).
fn parse_savepoint(cli: &Cli) -> Result<flink_explorer::parser::Savepoint, String> {
    let metadata = cli.savepoint_dir.join("_metadata");
    flink_explorer::parser::parse_metadata(&metadata)
        .map_err(|e| format!("parse {}: {}", metadata.display(), e))
}

/// The profile's protobuf context, built the way the TUI builds it.
fn proto_context(cli: &Cli) -> Option<flink_explorer::proto::ProtoContext> {
    cli.profile
        .as_deref()
        .and_then(|name| flink_explorer::profile::storage::load_profile(name).ok())
        .and_then(|p| flink_explorer::tui::app::build_proto_context(&p))
        .map(|(ctx, _loaded)| ctx)
}

#[cfg(feature = "web")]
fn web(cli: &Cli) -> Result<(), String> {
    // A key page renders at most this many entries per state, then links to the rest.
    let mut reader = flink_explorer::reader::StateReader::open(&cli.savepoint_dir)?
        .with_entry_limit(flink_explorer::web::ENTRY_LIMIT);
    if let Some(proto) = proto_context(cli) {
        reader = reader.with_proto(std::sync::Arc::new(proto));
    }
    let server = flink_explorer::web::serve(reader, std::net::SocketAddr::new(cli.bind, cli.port))?;
    if !cli.bind.is_loopback() {
        eprintln!(
            "WARNING: listening on {}, which is NOT loopback: anyone who can reach this address \
             and obtains the URL below can read this savepoint's data, over plain HTTP. Prefer \
             the default (127.0.0.1) with `ssh -L` for remote use.",
            server.addr()
        );
    }
    eprintln!(
        "Serving {} (read-only). Open this URL — it holds the session token, keep it private:",
        cli.savepoint_dir.display()
    );
    println!("{}", server.url());
    if cli.bind.is_unspecified() {
        eprintln!(
            "(Bound to every interface: from another machine, replace 127.0.0.1 in that URL with \
             an address of this one.)"
        );
    }
    eprintln!("Press Ctrl-C to stop.");
    if cli.open {
        open_browser(server.url());
    }
    server.wait();
    eprintln!("Stopped.");
    Ok(())
}

#[cfg(not(feature = "web"))]
fn web(_cli: &Cli) -> Result<(), String> {
    Err("this build has no web UI; rebuild with `--features web` (on by default)".to_string())
}

/// Best effort: a failure is reported and the server keeps running. The child is reaped by a
/// detached thread (no zombie while the server runs). The URL, token included, is on the child's
/// command line, which other local users can read (`ps`): the README says so.
#[cfg(feature = "web")]
fn open_browser(url: &str) {
    use std::process::{Command, Stdio};
    let mut cmd = if cfg!(target_os = "windows") {
        // Not `cmd /C start`: cmd would parse the URL, and `&` in it ends the command. The URL
        // handler takes it as one argument, unparsed.
        let mut c = Command::new("rundll32");
        c.args(["url.dll,FileProtocolHandler", url]);
        c
    } else if cfg!(target_os = "macos") {
        let mut c = Command::new("open");
        c.arg(url);
        c
    } else {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    match cmd.spawn() {
        Ok(mut child) => {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => eprintln!(
            "Could not open a browser ({}); open the URL above yourself.",
            e
        ),
    }
}

fn list_states(cli: &Cli) -> Result<(), String> {
    let savepoint = parse_savepoint(cli)?;
    let listing = flink_explorer::export_state::list_states(&savepoint, &cli.savepoint_dir);
    if !listing.states.is_empty() {
        println!("{:<34} {:<10} STATE", "OPERATOR", "TYPE");
        for (op, name, state_type) in &listing.states {
            println!("{:<34} {:<10} {}", op, state_type, name);
        }
    }
    // What could be read is listed; what could not makes the run fail, so that a script never
    // takes a partial list (or none at all) for the savepoint's content.
    if !listing.errors.is_empty() {
        for e in &listing.errors {
            eprintln!("Error: {}", e);
        }
        return Err(format!(
            "{} state file(s) could not be read, so this list may be incomplete",
            listing.errors.len()
        ));
    }
    if listing.states.is_empty() {
        eprintln!("No keyed state found in {}", cli.savepoint_dir.display());
    }
    Ok(())
}

fn export_state(cli: &Cli) -> Result<(), String> {
    let state = cli.export_state.as_deref().unwrap_or_default();
    let savepoint = parse_savepoint(cli)?;

    // Reuse the TUI's proto-context builder so protobuf-in-POJO decodes identically headless.
    let proto = proto_context(cli);

    let to_stdout = cli.out.as_deref().is_some_and(|p| p == Path::new("-"));
    let extension = match cli.format.as_str() {
        "ndjson" => "jsonl",
        "parquet" => "parquet",
        other => {
            return Err(format!(
                "unknown --format {:?}; expected \"ndjson\" or \"parquet\"",
                other
            ))
        }
    };
    let path = cli.out.clone().unwrap_or_else(|| {
        cli.savepoint_dir
            .join(format!("{}.{}", state.replace('/', "_"), extension))
    });

    let stats = match cli.format.as_str() {
        "ndjson" if to_stdout => {
            let stdout = io::stdout();
            let mut w = io::BufWriter::new(stdout.lock());
            flink_explorer::export_state::export_state_ndjson(
                &savepoint,
                &cli.savepoint_dir,
                cli.operator.as_deref(),
                state,
                proto.as_ref(),
                &mut w,
            )?
        }
        // Written to a side file and renamed on success, so a failed export (e.g. a state file
        // went missing) never truncates a previous good export at `path`.
        "ndjson" => write_then_rename(&path, |tmp| {
            let file = std::fs::File::create(tmp)
                .map_err(|e| format!("create {}: {}", tmp.display(), e))?;
            let mut w = io::BufWriter::new(file);
            let stats = flink_explorer::export_state::export_state_ndjson(
                &savepoint,
                &cli.savepoint_dir,
                cli.operator.as_deref(),
                state,
                proto.as_ref(),
                &mut w,
            )?;
            io::Write::flush(&mut w).map_err(|e| format!("write {}: {}", tmp.display(), e))?;
            Ok(stats)
        })?,
        "parquet" if to_stdout => {
            export_state_parquet(cli, &savepoint, state, proto.as_ref(), &path, to_stdout)?
        }
        "parquet" => write_then_rename(&path, |tmp| {
            export_state_parquet(cli, &savepoint, state, proto.as_ref(), tmp, false)
        })?,
        _ => unreachable!("format validated above"),
    };

    if !to_stdout {
        eprintln!(
            "Exported {} rows of {:?} to {} ({} subtasks, {} key-groups, peak block {} KiB{})",
            stats.rows,
            state,
            path.display(),
            stats.subtasks,
            stats.key_groups,
            stats.max_block_bytes / 1024,
            if stats.compressed {
                ", snappy key-groups"
            } else {
                ""
            }
        );
    }
    // On stderr even when the rows go to stdout: an incomplete export must not pass for a
    // complete one.
    if stats.malformed_key_groups > 0 {
        eprintln!(
            "Warning: {} malformed key-group(s); their entries past the defect were not exported \
             (first: {})",
            stats.malformed_key_groups,
            stats.first_malformed.as_deref().unwrap_or("?")
        );
    }
    Ok(())
}

fn is_stdout(cli: &Cli) -> bool {
    cli.out.as_deref().is_some_and(|p| p == Path::new("-"))
}

/// Run `write` against `<path>.partial`, then move it over `path` only if it succeeded.
fn write_then_rename<T>(
    path: &Path,
    write: impl FnOnce(&Path) -> Result<T, String>,
) -> Result<T, String> {
    // Only a regular file (or a path not there yet) can be replaced by a rename. `/dev/null`, a
    // named pipe or a device is written to directly: there is no previous export to protect.
    if std::fs::metadata(path).is_ok_and(|m| !m.is_file()) {
        return write(path);
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".partial");
    let tmp = PathBuf::from(tmp);
    match write(&tmp) {
        Ok(v) => {
            std::fs::rename(&tmp, path)
                .map_err(|e| format!("rename {} to {}: {}", tmp.display(), path.display(), e))?;
            Ok(v)
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

#[cfg(feature = "parquet")]
fn export_state_parquet(
    cli: &Cli,
    savepoint: &flink_explorer::parser::Savepoint,
    state: &str,
    proto: Option<&flink_explorer::proto::ProtoContext>,
    path: &Path,
    to_stdout: bool,
) -> Result<flink_explorer::export_state::ExportStats, String> {
    if to_stdout {
        // The writer must emit a footer, and a Parquet file on a pipe is rarely what anyone
        // wants; say so instead of producing something awkward.
        return Err("parquet output needs a file path, not stdout".to_string());
    }
    let mut sink = flink_explorer::export_parquet::ParquetSink::create(path)?;
    let stats = flink_explorer::export_state::export_state_rows(
        savepoint,
        &cli.savepoint_dir,
        cli.operator.as_deref(),
        state,
        proto,
        &mut sink,
    )?;
    eprintln!("value column: {:?}", sink.value_column());
    Ok(stats)
}

#[cfg(not(feature = "parquet"))]
fn export_state_parquet(
    _cli: &Cli,
    _savepoint: &flink_explorer::parser::Savepoint,
    _state: &str,
    _proto: Option<&flink_explorer::proto::ProtoContext>,
    _path: &Path,
    _to_stdout: bool,
) -> Result<flink_explorer::export_state::ExportStats, String> {
    Err("this build has no parquet support; rebuild with `--features parquet`".to_string())
}
