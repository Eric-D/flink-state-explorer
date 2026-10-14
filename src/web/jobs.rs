//! Count jobs: exact per-state key and entry counts of one operator, computed by a background
//! pass over its key-groups ([`crate::reader::StateReader::count`], a step of
//! [`super::WebOptions::count_step_bytes`] at a time, one block in memory). At most one job per
//! operator, and [`super::WebOptions::max_running_counts`] running over all operators (one more
//! is refused); results stay in memory for the session. A job checks for cancellation between
//! steps: a cancelled job is `cancelling` until its worker has stopped, and only then
//! `cancelled` (and restartable). Dropping the [`CountJobs`] (the server stopping) cancels
//! every job.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::reader::{Progress, StateCount, StateReader, Unreadable};

/// Warnings kept per job (the rest are counted, not listed).
const MAX_WARNINGS: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobStatus {
    Running,
    /// Asked to stop; its worker has not noticed yet.
    Cancelling,
    Done,
    Cancelled,
    Failed(String),
}

impl JobStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Running => "running",
            JobStatus::Cancelling => "cancelling",
            JobStatus::Done => "done",
            JobStatus::Cancelled => "cancelled",
            JobStatus::Failed(_) => "failed",
        }
    }

    /// Its worker is still running (it holds one of the running slots).
    pub(crate) fn is_live(&self) -> bool {
        matches!(self, JobStatus::Running | JobStatus::Cancelling)
    }
}

/// What a job has counted so far.
#[derive(Debug, Clone)]
pub(crate) struct CountSnapshot {
    pub status: JobStatus,
    /// Per state, in order of first appearance.
    pub states: Vec<StateCount>,
    pub keys: u64,
    pub progress: Progress,
    pub bytes_read: u64,
    pub unreadable: Vec<Unreadable>,
    pub malformed: Vec<String>,
    /// Warnings beyond [`MAX_WARNINGS`], not listed.
    pub more_warnings: usize,
    /// Key-groups skipped because they could not be read, all of them (listed or not).
    pub unreadable_key_groups: u64,
    /// Malformed key-groups (counted up to their defect), all of them.
    pub malformed_key_groups: u64,
    pub elapsed: Duration,
}

impl CountSnapshot {
    /// The counts are exact: every key-group was read, and read whole.
    pub fn exact(&self) -> bool {
        self.status == JobStatus::Done && self.incomplete_key_groups() == 0
    }

    /// Key-groups whose keys are missing from the counts, or partly missing.
    pub fn incomplete_key_groups(&self) -> u64 {
        self.unreadable_key_groups + self.malformed_key_groups
    }
}

struct Job {
    cancel: AtomicBool,
    started: Instant,
    state: Mutex<CountSnapshot>,
}

impl Job {
    fn snapshot(&self) -> CountSnapshot {
        let mut s = match self.state.lock() {
            Ok(s) => s.clone(),
            Err(p) => p.into_inner().clone(),
        };
        if s.status == JobStatus::Running {
            s.elapsed = self.started.elapsed();
        }
        s
    }

    fn update(&self, f: impl FnOnce(&mut CountSnapshot)) {
        let mut s = match self.state.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        f(&mut s);
    }

    fn finish(&self, status: JobStatus) {
        let elapsed = self.started.elapsed();
        self.update(|s| {
            s.status = status;
            s.elapsed = elapsed;
        });
    }
}

/// Why a count was not started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Busy(pub String);

/// The jobs of one server, by canonical hex operator id.
#[derive(Default)]
pub(crate) struct CountJobs {
    jobs: Mutex<HashMap<String, Arc<Job>>>,
}

impl CountJobs {
    /// The job of operator `op`, if one was started.
    pub(crate) fn get(&self, op: &str) -> Option<CountSnapshot> {
        let jobs = self.jobs.lock().ok()?;
        jobs.get(op).map(|j| j.snapshot())
    }

    /// The job of `op`, started if there is none — or, with `restart`, if the one there has
    /// stopped (a job whose worker still runs, even cancelling, is never started twice). `Err`
    /// when `max_running` jobs already run.
    pub(crate) fn start(
        &self,
        reader: &Arc<StateReader<'static>>,
        op: &str,
        restart: bool,
        options: &super::WebOptions,
    ) -> Result<CountSnapshot, Busy> {
        let (step_bytes, pause) = (options.count_step_bytes, options.count_step_pause);
        let mut jobs = match self.jobs.lock() {
            Ok(j) => j,
            Err(p) => p.into_inner(),
        };
        if let Some(j) = jobs.get(op) {
            let snap = j.snapshot();
            if !restart || snap.status.is_live() {
                return Ok(snap);
            }
        }
        let running = jobs
            .values()
            .filter(|j| j.snapshot().status.is_live())
            .count();
        let max_running = options.max_running_counts.max(1);
        if running >= max_running {
            return Err(Busy(format!(
                "{} count(s) already running, the most this server runs at once; cancel one or \
                 retry when one has finished",
                running
            )));
        }
        let initial = reader.progress(op, Some(&Default::default()), 0, None);
        let job = Arc::new(Job {
            cancel: AtomicBool::new(false),
            started: Instant::now(),
            state: Mutex::new(CountSnapshot {
                status: JobStatus::Running,
                states: Vec::new(),
                keys: 0,
                progress: initial.unwrap_or_default(),
                bytes_read: 0,
                unreadable: Vec::new(),
                malformed: Vec::new(),
                more_warnings: 0,
                unreadable_key_groups: 0,
                malformed_key_groups: 0,
                elapsed: Duration::ZERO,
            }),
        });
        jobs.insert(op.to_string(), job.clone());
        let snap = job.snapshot();
        let reader = reader.clone();
        let op = op.to_string();
        let spawned = std::thread::Builder::new()
            .name("flink-explorer-count".into())
            .spawn({
                let job = job.clone();
                move || {
                    let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run(&reader, &op, &job, step_bytes, pause)
                    }));
                    if ran.is_err() {
                        job.finish(JobStatus::Failed(
                            "the count failed (internal error)".into(),
                        ));
                    }
                }
            });
        if let Err(e) = spawned {
            job.finish(JobStatus::Failed(format!("cannot start the count: {}", e)));
            return Ok(job.snapshot());
        }
        Ok(snap)
    }

    /// Ask `op`'s job to stop (idempotent: a finished job stays as it is). It is `cancelling`
    /// until its worker notices, between two steps. `None`: no job.
    pub(crate) fn cancel(&self, op: &str) -> Option<CountSnapshot> {
        let job = self.jobs.lock().ok()?.get(op).cloned()?;
        job.cancel.store(true, Ordering::SeqCst);
        job.update(|s| {
            if s.status == JobStatus::Running {
                s.status = JobStatus::Cancelling;
            }
        });
        Some(job.snapshot())
    }
}

impl Drop for CountJobs {
    fn drop(&mut self) {
        if let Ok(jobs) = self.jobs.lock() {
            for j in jobs.values() {
                j.cancel.store(true, Ordering::SeqCst);
            }
        }
    }
}

fn run(reader: &StateReader<'static>, op: &str, job: &Job, step_bytes: u64, pause: Duration) {
    let mut cursor = match reader.cursor_at(op, 0) {
        Ok(c) => c,
        Err(e) => return job.finish(JobStatus::Failed(e.to_string())),
    };
    loop {
        if job.cancel.load(Ordering::SeqCst) {
            return job.finish(JobStatus::Cancelled);
        }
        let page = match reader.count(op, &cursor, step_bytes.max(1)) {
            Ok(p) => p,
            Err(e) => return job.finish(JobStatus::Failed(e.to_string())),
        };
        let progress = reader.progress(op, page.next.as_ref(), 0, None);
        job.update(|s| {
            for c in page.states {
                match s.states.iter_mut().find(|x| x.state == c.state) {
                    Some(x) => {
                        x.keys += c.keys;
                        x.entries += c.entries;
                    }
                    None => s.states.push(c),
                }
            }
            s.keys += page.keys;
            s.bytes_read += page.stats.bytes_read;
            s.unreadable_key_groups += page
                .unreadable
                .iter()
                .map(|u| u64::from(u.last_key_group.saturating_sub(u.first_key_group)) + 1)
                .sum::<u64>();
            s.malformed_key_groups += page.malformed.len() as u64;
            for u in page.unreadable {
                if s.unreadable.len() + s.malformed.len() < MAX_WARNINGS {
                    s.unreadable.push(u);
                } else {
                    s.more_warnings += 1;
                }
            }
            for m in page.malformed {
                if s.unreadable.len() + s.malformed.len() < MAX_WARNINGS {
                    s.malformed.push(m);
                } else {
                    s.more_warnings += 1;
                }
            }
            if let Ok(p) = progress {
                s.progress = p;
            }
        });
        match page.next {
            None => {
                // A cancel that raced the last step loses: the counts are complete.
                job.cancel.store(false, Ordering::SeqCst);
                return job.finish(JobStatus::Done);
            }
            Some(next) => cursor = next,
        }
        if !pause.is_zero() {
            std::thread::sleep(pause);
        }
    }
}
