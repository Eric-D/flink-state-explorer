//! Background work for the TUI: the key count pass and the `E` export. Each runs on a worker
//! thread over a shared [`StateReader`] (it is `Send + Sync`), reports through a channel the
//! event loop drains between frames, and stops at its next step when cancelled — on `q`, Ctrl-C,
//! or when the [`Job`] is dropped (which also joins the thread, so nothing outlives the app).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::reader::{Cursor, StateReader};

/// A worker thread sending `M`s.
pub struct Job<M> {
    rx: Receiver<M>,
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<M: Send + 'static> Job<M> {
    /// Run `work(tx, cancel)` on a new thread.
    pub fn spawn(work: impl FnOnce(Sender<M>, &AtomicBool) + Send + 'static) -> Job<M> {
        let (tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let flag = cancel.clone();
        let handle = std::thread::spawn(move || work(tx, &flag));
        Job {
            rx,
            cancel,
            handle: Some(handle),
        }
    }

    /// The messages sent since the last call, without blocking.
    pub fn drain(&self) -> Vec<M> {
        self.rx.try_iter().collect()
    }

    /// Whether the thread is still working.
    pub fn is_running(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// Ask the worker to stop at its next step, and wait for it.
    pub fn cancel(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    /// Wait for the worker to finish on its own (tests).
    pub fn join(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl<M> Drop for Job<M> {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Stored bytes of blocks one count step reads before it checks for cancellation and reports.
pub const COUNT_STEP_BYTES: u64 = 4 << 20;

/// What the count pass reports, per operator (indices into the list it was started with).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountMsg {
    /// `keys` counted so far, `percent` of the operator's key-groups passed.
    Progress { op: usize, keys: usize, percent: u8 },
    /// The operator is counted. `complete` is false when some key-group could not be read (or
    /// was malformed): `keys` is then a lower bound.
    Done {
        op: usize,
        keys: usize,
        complete: bool,
    },
}

/// One operator for the count pass: its index in the caller's list, its hex id, its
/// maxParallelism (for the progress percentage).
#[derive(Debug, Clone)]
pub struct CountTarget {
    pub op: usize,
    pub hex: String,
    pub max_parallelism: i32,
}

/// Count every key of every target, one operator after the other, with
/// [`StateReader::scan`] and a predicate that matches nothing (so no record is built): each step
/// reads about `step_bytes` of blocks.
pub fn start_count(
    reader: Arc<StateReader<'static>>,
    targets: Vec<CountTarget>,
    step_bytes: u64,
) -> Job<CountMsg> {
    Job::spawn(move |tx, cancel| {
        for t in targets {
            let mut keys = 0usize;
            let mut complete = true;
            let mut cursor = Some(Cursor::START);
            while let Some(c) = cursor {
                if cancel.load(Ordering::Relaxed) {
                    return;
                }
                let page = match reader.scan(&t.hex, &c, step_bytes, &mut |_| {
                    keys += 1;
                    false
                }) {
                    Ok(p) => p,
                    Err(_) => {
                        complete = false;
                        break;
                    }
                };
                if !page.unreadable.is_empty() || !page.malformed.is_empty() {
                    complete = false;
                }
                cursor = page.next;
                if let Some(next) = cursor {
                    let percent = (next.key_group as i64 * 100 / t.max_parallelism.max(1) as i64)
                        .clamp(0, 99) as u8;
                    if tx
                        .send(CountMsg::Progress {
                            op: t.op,
                            keys,
                            percent,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if tx
                .send(CountMsg::Done {
                    op: t.op,
                    keys,
                    complete,
                })
                .is_err()
            {
                return;
            }
        }
    })
}

/// What the `E` export reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportMsg {
    Progress(usize),
    Done(Result<crate::export::ExportSummary, String>),
}

/// Run [`crate::export::export_json`] on a worker thread.
pub fn start_export(
    reader: Arc<StateReader<'static>>,
    operators: Vec<crate::operators::OperatorSummary>,
    output: std::path::PathBuf,
) -> Job<ExportMsg> {
    Job::spawn(move |tx, cancel| {
        let mut last = 0usize;
        let progress_tx = tx.clone();
        let result = crate::export::export_json(&reader, &operators, &output, cancel, &mut |n| {
            if n >= last + 10_000 {
                last = n;
                let _ = progress_tx.send(ExportMsg::Progress(n));
            }
        });
        let _ = tx.send(ExportMsg::Done(result));
    })
}
