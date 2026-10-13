//! Entering the TUI's terminal mode (raw mode + alternate screen), and always leaving it.
//!
//! Without this, a panic left the shell in raw mode (no echo, no Ctrl-C) on the alternate screen,
//! with the panic message lost on that screen: the terminal was restored only on a normal return.
//! [`TerminalGuard`] restores it on every way out — return, `?`, panic — and does it *before* the
//! panic message is printed, so the message lands on the normal screen.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossterm::cursor::Show;
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};

/// Holds the terminal in TUI mode; dropping it restores the terminal.
pub struct TerminalGuard {
    /// True while the terminal still needs restoring. Shared with the panic hook so that the
    /// terminal is restored exactly once, by whichever of the hook and `Drop` comes first.
    active: Arc<AtomicBool>,
    restore: fn(),
}

impl TerminalGuard {
    /// Install the panic hook, then enable raw mode and enter the alternate screen.
    pub fn enter() -> io::Result<Self> {
        Self::enter_with(enter_tui_mode, restore_terminal)
    }

    /// [`enter`](Self::enter) with the two terminal operations given, so that tests can check
    /// the ordering without a terminal. The panic hook is installed **before** `enter` runs, so a
    /// panic at any point after raw mode is enabled is covered; it calls `restore`, then the
    /// previously installed hook (by default, the one that prints the panic message). If `enter`
    /// fails, whatever it managed to set up is restored when the guard is dropped.
    pub fn enter_with(enter: impl FnOnce() -> io::Result<()>, restore: fn()) -> io::Result<Self> {
        let active = Arc::new(AtomicBool::new(true));
        let pending = Arc::clone(&active);
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if pending.swap(false, Ordering::SeqCst) {
                restore();
            }
            previous(info);
        }));
        let guard = TerminalGuard { active, restore };
        enter()?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::SeqCst) {
            (self.restore)();
        }
    }
}

fn enter_tui_mode() -> io::Result<()> {
    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)
}

/// Leave TUI mode: raw mode off, back to the normal screen, cursor shown. Best effort and safe to
/// call when the terminal is not in TUI mode. Call it directly only before `process::exit`,
/// which skips the guard's `Drop`.
pub fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen, Show);
}
