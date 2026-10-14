//! Background work for the TUI: the key count pass, the search (`/`) and the `E` export. Each
//! runs on a worker thread over a shared [`StateReader`] (it is `Send + Sync`), reports through a
//! channel the event loop drains between frames, and stops at its next step when cancelled — on
//! `Esc` (a search), `q` (confirmed while an export runs), Ctrl-C, or when the [`Job`] is dropped
//! (which also joins the thread, so nothing outlives the app).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::reader::{Cursor, KeyRecord, ReaderError, StateReader, Unreadable};
use crate::tui::filter::AppliedFilter;

/// A worker thread sending `M`s.
pub struct Job<M> {
    rx: Receiver<M>,
    /// Messages received while waiting for the worker to end, for [`Job::drain`].
    pending: std::collections::VecDeque<M>,
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<M: Send + 'static> Job<M> {
    /// Run `work(tx, cancel)` on a new thread.
    pub fn spawn(work: impl FnOnce(Sender<M>, &AtomicBool) + Send + 'static) -> Job<M> {
        let (tx, rx) = mpsc::channel();
        Self::start(rx, move |flag| work(tx, flag))
    }

    /// Run `work(tx, cancel)` on a new thread, sending through a channel of `capacity` messages:
    /// the worker waits while the channel is full, so it never runs more than that ahead of the
    /// event loop (a search's matches do not pile up in memory).
    pub fn spawn_bounded(
        capacity: usize,
        work: impl FnOnce(SyncSender<M>, &AtomicBool) + Send + 'static,
    ) -> Job<M> {
        let (tx, rx) = mpsc::sync_channel(capacity);
        Self::start(rx, move |flag| work(tx, flag))
    }

    fn start(rx: Receiver<M>, work: impl FnOnce(&AtomicBool) + Send + 'static) -> Job<M> {
        let cancel = Arc::new(AtomicBool::new(false));
        let flag = cancel.clone();
        let handle = std::thread::spawn(move || work(&flag));
        Job {
            rx,
            pending: Default::default(),
            cancel,
            handle: Some(handle),
        }
    }

    /// The messages sent since the last call, without blocking.
    pub fn drain(&mut self) -> Vec<M> {
        let mut out: Vec<M> = self.pending.drain(..).collect();
        out.extend(self.rx.try_iter());
        out
    }

    /// The next message, waiting for it; `None` once the worker has ended and every message it
    /// sent was received.
    pub fn recv(&mut self) -> Option<M> {
        match self.pending.pop_front() {
            Some(m) => Some(m),
            None => self.rx.recv().ok(),
        }
    }

    /// Whether the thread is still working.
    pub fn is_running(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// Ask the worker to stop at its next step, and wait for it. What it sends meanwhile is kept
    /// for [`Job::drain`] (receiving it is also what frees a worker waiting on a full channel).
    pub fn cancel(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        self.wait(true);
    }

    /// Wait for the worker to finish on its own (tests).
    pub fn join(&mut self) {
        self.wait(true);
    }
}

impl<M> Job<M> {
    /// Wait for the worker to end, receiving what it sends meanwhile (kept if `keep`).
    fn wait(&mut self, keep: bool) {
        let Some(h) = self.handle.take() else {
            return;
        };
        while !h.is_finished() {
            match self.rx.recv_timeout(Duration::from_millis(5)) {
                Ok(m) if keep => self.pending.push_back(m),
                Ok(_) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        let _ = h.join();
    }
}

impl<M> Drop for Job<M> {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        self.wait(false);
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

/// What the search reports, per operator (indices into the tree's operators).
#[derive(Debug, Clone, PartialEq)]
pub enum SearchMsg {
    /// The matches of a stretch of the operator's keys, and where the search goes on from
    /// (`None`: the operator is searched to its end).
    Page {
        op: usize,
        matches: Vec<KeyRecord>,
        next: Option<Cursor>,
        unreadable: Vec<Unreadable>,
        malformed: Vec<String>,
    },
    /// The operator could not be searched further.
    Failed { op: usize, error: ReaderError },
}

/// One operator to search: its index in the tree, its hex id, where to start.
#[derive(Debug, Clone)]
pub struct SearchTarget {
    pub op: usize,
    pub hex: String,
    pub from: Cursor,
}

/// Reports the search worker may run ahead of the event loop.
const SEARCH_CHANNEL: usize = 4;

/// Matches gathered before they are sent, whatever was read.
const SEARCH_BATCH_MATCHES: usize = 1000;

/// Search the keys of every target for `filter`, one operator after the other, with
/// [`StateReader::scan`] **one block per call**, so that a cancel is seen between any two blocks
/// (one key-group is still read whole). The matches are sent once about `step_bytes` of blocks
/// were read, or [`SEARCH_BATCH_MATCHES`] found, and at the end of each operator.
pub fn start_search(
    reader: Arc<StateReader<'static>>,
    targets: Vec<SearchTarget>,
    filter: AppliedFilter,
    step_bytes: u64,
) -> Job<SearchMsg> {
    Job::spawn_bounded(SEARCH_CHANNEL, move |tx, cancel| {
        let exact = match &filter {
            AppliedFilter::Exact(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        };
        let mut matches = |key: &str| match &exact {
            Some(e) => key == e,
            None => filter.matches_str(key, 0),
        };
        for t in targets {
            let mut cursor = Some(t.from);
            let mut found = Vec::new();
            let mut unreadable = Vec::new();
            let mut malformed = Vec::new();
            let mut bytes = 0u64;
            while let Some(c) = cursor {
                if cancel.load(Ordering::Relaxed) {
                    return;
                }
                let page = match reader.scan(&t.hex, &c, 1, &mut matches) {
                    Ok(p) => p,
                    Err(error) => {
                        if tx.send(SearchMsg::Failed { op: t.op, error }).is_err() {
                            return;
                        }
                        break;
                    }
                };
                bytes += page.stats.block_bytes.max(1);
                found.extend(page.matches);
                unreadable.extend(page.unreadable);
                malformed.extend(page.malformed);
                cursor = page.next;
                if cursor.is_none() || bytes >= step_bytes || found.len() >= SEARCH_BATCH_MATCHES {
                    let msg = SearchMsg::Page {
                        op: t.op,
                        matches: std::mem::take(&mut found),
                        next: cursor,
                        unreadable: std::mem::take(&mut unreadable),
                        malformed: std::mem::take(&mut malformed),
                    };
                    if tx.send(msg).is_err() {
                        return;
                    }
                    bytes = 0;
                }
            }
        }
    })
}

/// What the `E` export reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportMsg {
    Progress(crate::export::ExportProgress),
    Done(Result<crate::export::ExportSummary, String>),
}

/// Run [`crate::export::export_json`] on a worker thread.
pub fn start_export(
    reader: Arc<StateReader<'static>>,
    operators: Vec<crate::operators::OperatorSummary>,
    output: std::path::PathBuf,
) -> Job<ExportMsg> {
    Job::spawn(move |tx, cancel| {
        let mut last: Option<crate::export::ExportProgress> = None;
        let progress_tx = tx.clone();
        let result = crate::export::export_json(&reader, &operators, &output, cancel, &mut |p| {
            // Every operator done, and every 10 000 keys.
            let report = last
                .is_none_or(|l| p.operators_done != l.operators_done || p.keys >= l.keys + 10_000);
            if report {
                last = Some(p);
                let _ = progress_tx.send(ExportMsg::Progress(p));
            }
        });
        let _ = tx.send(ExportMsg::Done(result));
    })
}
