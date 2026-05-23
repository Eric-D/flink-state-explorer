use std::io::{self, Stdout};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::prelude::*;
use ratatui::Terminal;

use flink_explorer::error::{AppError, UiError};
use flink_explorer::tui::app::App;
use flink_explorer::tui::loading::{LoadingScreen, LoadingStep};

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

    /// Skip index cache (always re-parse)
    #[arg(long)]
    no_cache: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

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
    // Enter TUI immediately so we can show loading progress
    enable_raw_mode().map_err(UiError::Init)?;
    io::stdout()
        .execute(EnterAlternateScreen)
        .map_err(UiError::Init)?;

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).map_err(UiError::Init)?;

    let result = load_and_run(cli, &mut terminal);

    // Always restore terminal
    disable_raw_mode().ok();
    io::stdout().execute(LeaveAlternateScreen).ok();

    result
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
    })?;

    loading.advance(LoadingStep::Done);
    loading.render(terminal)?;

    app.run_with_terminal(terminal)
}
