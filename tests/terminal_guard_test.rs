//! `TerminalGuard` restores the terminal on a panic, before the panic message is printed (#14).
//!
//! The panic hook is process-global, so this file holds a single test: no other test of this
//! binary can install a hook or panic concurrently. The real terminal operations are replaced by
//! counters (`enter_with`), so this needs no terminal and runs in CI.

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use flink_explorer::tui::terminal::TerminalGuard;

/// Times the terminal was restored.
static RESTORES: AtomicUsize = AtomicUsize::new(0);
/// Times the previously installed hook (the one that prints the message) ran.
static MESSAGES: AtomicUsize = AtomicUsize::new(0);
/// `RESTORES` as the previous hook saw it: the terminal must already be restored by then, or
/// the message is printed on the alternate screen, in raw mode, and lost.
static RESTORES_AT_MESSAGE: AtomicUsize = AtomicUsize::new(usize::MAX);

fn restore() {
    RESTORES.fetch_add(1, SeqCst);
}

fn counts() -> (usize, usize, usize) {
    (
        RESTORES.load(SeqCst),
        MESSAGES.load(SeqCst),
        RESTORES_AT_MESSAGE.load(SeqCst),
    )
}

#[test]
fn a_panic_restores_the_terminal_then_runs_the_previous_hook() {
    // Records when the hook that prints "thread panicked at ..." runs, then prints it (so a
    // failed assertion below still says why).
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        MESSAGES.fetch_add(1, SeqCst);
        RESTORES_AT_MESSAGE.store(RESTORES.load(SeqCst), SeqCst);
        default(info);
    }));

    // A panic while the TUI runs: restored once (the hook, then `Drop` during unwinding finds it
    // done), and before the previous hook prints.
    let result = std::panic::catch_unwind(|| {
        let _guard = TerminalGuard::enter_with(|| Ok(()), restore).unwrap();
        panic!("boom in the event loop");
    });
    assert!(result.is_err());
    assert_eq!(counts(), (1, 1, 1));

    // A panic right after raw mode is enabled, before the alternate screen: the hook is already
    // installed when `enter` runs, so this is covered too (and still before the message).
    let result = std::panic::catch_unwind(|| {
        let _guard = TerminalGuard::enter_with(
            || panic!("boom between raw mode and the alternate screen"),
            restore,
        );
    });
    assert!(result.is_err());
    assert_eq!(counts(), (2, 2, 2));

    // `enter` fails: whatever it set up is restored, and the error comes back.
    let failed = TerminalGuard::enter_with(|| Err(std::io::Error::other("no tty")), restore);
    assert!(failed.is_err());
    assert_eq!(RESTORES.load(SeqCst), 3);

    // A normal exit restores through `Drop`, once.
    {
        let _guard = TerminalGuard::enter_with(|| Ok(()), restore).unwrap();
        assert_eq!(RESTORES.load(SeqCst), 3);
    }
    assert_eq!(RESTORES.load(SeqCst), 4);

    // After the TUI has left, a panic is not a reason to touch the terminal again; the hooks
    // still chain down to the previous one.
    let result = std::panic::catch_unwind(|| panic!("after the TUI"));
    assert!(result.is_err());
    assert_eq!(RESTORES.load(SeqCst), 4);
    assert_eq!(MESSAGES.load(SeqCst), 3);

    drop(std::panic::take_hook()); // back to the default hook
}
