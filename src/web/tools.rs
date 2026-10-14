//! Search, counts and downloads: the routes that read a whole operator or state, each bounded
//! per request.
//!
//! - **Search** (`/op/<id>/search`, JSON `/api/op/<id>/search`): one [`StateReader::scan_with`]
//!   per request, under a byte budget and a match cap, resumable from its `next` cursor. The page
//!   works as "next page of results" links; `app.js` calls the JSON twin from cursor to cursor and
//!   appends rows as they arrive (text only), with progress from `_metadata`'s offsets and a Stop
//!   button (it stops issuing requests). Optionally restricted to a key-group range.
//! - **Counts** (`/op/<id>/count`, JSON twin polled by `app.js`): a background job
//!   ([`super::jobs`]) per operator; `?cancel=1` stops it, `?restart=1` starts a stopped one again.
//!   A `HEAD` shows a job, never starts nor cancels one.
//! - **Downloads** (`/op/<id>/download?state=&format=`): the state streamed through
//!   [`export_state_rows`] and a [`RowSink`] — byte for byte what `--export-state -o -` writes —
//!   as a chunked body produced on a blocking thread that holds a download slot
//!   ([`super::http`]).

use serde_json::{json, Value};

use super::html::Html;
use super::jobs::{Busy, CountSnapshot, JobStatus};
use super::pages::{
    check_state, href, json_response, layout, link, malformed_warnings, op_crumb, op_path, resolve,
    stats_json, summary, unreadable_json, unreadable_warnings, Fail, Query, Res, CELL_CHARS,
    DEFAULT_LIMIT,
};
use super::{App, Response, Streamed};
use crate::export_state::{export_state_rows, NdjsonSink, RowSink};
use crate::glob::{KeyMatcher, MatchMode};
use crate::reader::{Cursor, Progress, ScanOptions, ScanPage};

/// Longest query accepted, in characters.
pub const MAX_QUERY_CHARS: usize = 256;
/// Stored bytes one search request reads unless `budget` says otherwise (a few key-groups of a
/// large savepoint), and the most it may ask for.
pub const DEFAULT_SEARCH_BUDGET: u64 = 4 << 20;
pub const MAX_SEARCH_BUDGET: u64 = 64 << 20;

// --- search ---------------------------------------------------------------------------------------

/// A parsed search request.
struct Search {
    id: String,
    q: String,
    mode: MatchMode,
    from: Option<u16>,
    to: Option<u16>,
    budget: u64,
    limit: usize,
    /// Where this request starts.
    cursor: Cursor,
    /// Whether `cursor` was given (a continuation).
    resumed: bool,
}

impl Search {
    /// The query parameters of this search, at `cursor` (`None`: from its start).
    fn params(&self, cursor: Option<&Cursor>) -> Vec<(&'static str, String)> {
        let mut p = vec![
            ("q", self.q.clone()),
            ("mode", self.mode.as_str().to_string()),
        ];
        if let Some(f) = self.from {
            p.push(("from", f.to_string()));
        }
        if let Some(t) = self.to {
            p.push(("to", t.to_string()));
        }
        if self.budget != DEFAULT_SEARCH_BUDGET {
            p.push(("budget", self.budget.to_string()));
        }
        if self.limit != DEFAULT_LIMIT {
            p.push(("limit", self.limit.to_string()));
        }
        if let Some(c) = cursor {
            p.push(("cursor", c.to_string()));
        }
        p
    }

    fn url(&self, api: bool, cursor: Option<&Cursor>) -> String {
        let base = op_path(&self.id, "search");
        let base = if api { format!("/api{}", base) } else { base };
        let p = self.params(cursor);
        let refs: Vec<(&str, &str)> = p.iter().map(|(k, v)| (*k, v.as_str())).collect();
        href(&base, &refs)
    }
}

/// `op` resolved, and holding keyed state (else the reader's `NoKeyedState`, a 404): a search
/// or a count of a non-keyed operator is not an empty result.
fn keyed(app: &App, op: &str) -> Res<(usize, String)> {
    let (i, id) = resolve(app, op)?;
    let has_keyed = app
        .reader
        .operators()
        .get(i)
        .is_some_and(|o| o.keyed_subtasks > 0);
    if !has_keyed {
        return Err(crate::reader::ReaderError::NoKeyedState { op: id }.into());
    }
    Ok((i, id))
}

fn opt_u16(q: &Query, name: &str) -> Res<Option<u16>> {
    match q.get(name) {
        None | Some("") => Ok(None),
        Some(v) => v.trim().parse::<u16>().map(Some).map_err(|_| {
            Fail::BadRequest(format!("{} must be a key-group number (0..65535)", name))
        }),
    }
}

/// `Ok(None)`: no query yet (the HTML page then shows the form only).
fn parse_search(app: &App, op: &str, q: &Query) -> Res<(String, Option<Search>)> {
    let (i, id) = keyed(app, op)?;
    let query = match q.get("q") {
        None | Some("") => return Ok((id, None)),
        Some(s) => s.to_string(),
    };
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(Fail::BadRequest(format!(
            "the query is longer than {} characters",
            MAX_QUERY_CHARS
        )));
    }
    let mode = match q.get("mode") {
        None | Some("") => MatchMode::Glob,
        Some(m) => MatchMode::parse(m).ok_or_else(|| {
            Fail::BadRequest("mode must be \"glob\", \"substring\" or \"exact\"".into())
        })?,
    };
    let max_p = app.reader.savepoint().operators[i].max_parallelism.max(1);
    let last = (max_p - 1) as u16;
    let from = opt_u16(q, "from")?;
    let to = opt_u16(q, "to")?;
    for v in [from, to].into_iter().flatten() {
        if v > last {
            return Err(Fail::BadRequest(format!(
                "key-group {} is out of range: this operator has key-groups 0..{}",
                v, last
            )));
        }
    }
    if let (Some(f), Some(t)) = (from, to) {
        if f > t {
            return Err(Fail::BadRequest(format!(
                "empty key-group range {}..{}",
                f, t
            )));
        }
    }
    let budget = match q.get("budget") {
        None | Some("") => DEFAULT_SEARCH_BUDGET,
        Some(b) => match b.parse::<u64>() {
            Ok(n) if (1..=MAX_SEARCH_BUDGET).contains(&n) => n,
            _ => {
                return Err(Fail::BadRequest(format!(
                    "budget must be between 1 and {} bytes",
                    MAX_SEARCH_BUDGET
                )))
            }
        },
    };
    let limit = q.limit()?;
    let (cursor, resumed) = match q.cursor("cursor")? {
        Some(c) => {
            if from.is_some_and(|f| c.key_group < f) || to.is_some_and(|t| c.key_group > t) {
                return Err(Fail::BadRequest(format!(
                    "cursor {} is outside the key-group range",
                    c
                )));
            }
            (c, true)
        }
        None => (app.reader.cursor_at(&id, from.unwrap_or(0))?, false),
    };
    Ok((
        id.clone(),
        Some(Search {
            id,
            q: query,
            mode,
            from,
            to,
            budget,
            limit,
            cursor,
            resumed,
        }),
    ))
}

fn run_search(app: &App, s: &Search) -> Res<(ScanPage, Progress)> {
    let matcher = KeyMatcher::new(s.mode, &s.q);
    let opts = ScanOptions {
        byte_budget: s.budget,
        max_matches: s.limit,
        last_key_group: s.to,
    };
    let page = app
        .reader
        .scan_with(&s.id, &s.cursor, &opts, &mut |k| matcher.matches(k))?;
    let progress = app
        .reader
        .progress(&s.id, page.next.as_ref(), s.from.unwrap_or(0), s.to)?;
    Ok((page, progress))
}

fn progress_json(p: &Progress) -> Value {
    json!({
        "key_groups_done": p.key_groups_done,
        "key_groups_total": p.key_groups_total,
        "bytes_done": p.bytes_done,
        "bytes_total": p.bytes_total,
        "percent": (p.percent() * 10.0).round() / 10.0,
    })
}

fn progress_text(p: &Progress) -> String {
    format!(
        "{} of {} key-groups, {} of {} bytes ({:.1} %)",
        p.key_groups_done,
        p.key_groups_total,
        p.bytes_done,
        p.bytes_total,
        p.percent()
    )
}

pub(super) fn api_search(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (_, search) = parse_search(app, op, q)?;
    let s = search.ok_or_else(|| Fail::BadRequest("missing parameter \"q\"".into()))?;
    let (page, progress) = run_search(app, &s)?;
    let matches: Vec<Value> = page
        .matches
        .iter()
        .map(|rec| {
            json!({
                "key": rec.key,
                "key_group": rec.key_group,
                "subtask": rec.subtask,
                "position": rec.position.to_string(),
                "href": href(&op_path(&s.id, "key"), &[("at", &rec.position.to_string())]),
                "summary": summary(rec, CELL_CHARS),
            })
        })
        .collect();
    Ok(json_response(
        200,
        &json!({
            "operator": s.id,
            "q": s.q,
            "mode": s.mode.as_str(),
            "from": s.from,
            "to": s.to,
            "cursor": s.cursor.to_string(),
            "matches": matches,
            "done": page.next.is_none(),
            "next": page.next.map(|c| c.to_string()),
            "next_api": page.next.map(|c| s.url(true, Some(&c))),
            "next_page": page.next.map(|c| s.url(false, Some(&c))),
            "progress": progress_json(&progress),
            "progress_text": progress_text(&progress),
            "malformed": page.malformed,
            "unreadable": unreadable_json(&page.unreadable),
            "stats": stats_json(&page.stats),
        }),
    ))
}

/// The search form; also on the operator page.
pub(super) fn search_form(
    h: &mut Html,
    id: &str,
    q: &str,
    mode: MatchMode,
    range: (Option<u16>, Option<u16>),
) {
    h.raw("<form class=\"lookup search\" method=\"get\" action=\"")
        .text(&op_path(id, "search"))
        .raw("\"><label>Search keys <input type=\"search\" name=\"q\" value=\"")
        .text(q)
        .raw("\" maxlength=\"256\" placeholder=\"glob: user-*, *42*, a?c\" required></label> ")
        .raw("<select name=\"mode\" aria-label=\"match\">");
    for m in [MatchMode::Glob, MatchMode::Substring, MatchMode::Exact] {
        h.raw("<option value=\"").text(m.as_str()).raw("\"");
        if m == mode {
            h.raw(" selected");
        }
        h.raw(">").text(m.as_str()).raw("</option>");
    }
    h.raw("</select> <label class=\"range\">key-groups <input type=\"number\" name=\"from\" min=\"0\" max=\"65535\" value=\"");
    if let Some(f) = range.0 {
        h.display(f);
    }
    h.raw("\" placeholder=\"from\"> – <input type=\"number\" name=\"to\" min=\"0\" max=\"65535\" value=\"");
    if let Some(t) = range.1 {
        h.display(t);
    }
    h.raw("\" placeholder=\"to\"></label> <button type=\"submit\">Search</button></form>");
}

pub(super) fn search_page(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, search) = parse_search(app, op, q)?;
    let mut h = Html::new();
    h.raw("<h1>Search keys</h1>");
    let crumbs = [op_crumb(&id), (String::new(), "search".to_string())];
    let Some(s) = search else {
        search_form(&mut h, &id, "", MatchMode::Glob, (None, None));
        h.raw(
            "<p class=\"note\">Every key's rendered text is tested, key-group by key-group, a few \
             megabytes per request. <strong>glob</strong>: <code>*</code> any run of \
             characters, <code>?</code> one, case ignored; <strong>substring</strong>: the text \
             anywhere in the key, case ignored; <strong>exact</strong>: the key itself (for key \
             types the lookup cannot locate).</p>",
        );
        return Ok(layout(app, "Search", &crumbs, &h));
    };
    search_form(&mut h, &id, &s.q, s.mode, (s.from, s.to));
    let (page, progress) = run_search(app, &s)?;

    // `app.js` continues from `data-api` (the next request) while it is set; without it, the
    // "next page" link below does the same, one click per request.
    h.raw("<p id=\"search-status\" class=\"meta\"");
    if let Some(next) = &page.next {
        h.raw(" data-api=\"")
            .text(&s.url(true, Some(next)))
            .raw("\"");
    }
    h.raw(">");
    if s.resumed {
        h.raw("Resumed at ").display(s.cursor).raw(". ");
    }
    h.display(page.matches.len())
        .raw(" match(es) on this page · ")
        .text(&progress_text(&progress))
        .raw(if page.next.is_none() {
            " · search complete."
        } else {
            ""
        })
        .raw("</p><p><button type=\"button\" id=\"search-stop\" hidden>Stop</button></p>");
    h.raw("<div id=\"search-warnings\">");
    unreadable_warnings(&mut h, &page.unreadable);
    malformed_warnings(&mut h, &page.malformed);
    h.raw("</div>");
    h.raw("<table class=\"keys\" id=\"search-results\"><thead><tr><th>Key</th><th>States</th>")
        .raw("<th>Key-group</th><th>Subtask</th></tr></thead><tbody>");
    for rec in &page.matches {
        h.raw("<tr><td class=\"key\">");
        link(
            &mut h,
            &href(&op_path(&id, "key"), &[("at", &rec.position.to_string())]),
            &rec.key,
        );
        h.raw("</td><td class=\"val\">")
            .text(&summary(rec, CELL_CHARS))
            .raw("</td><td>")
            .display(rec.key_group)
            .raw("</td><td>")
            .display(rec.subtask)
            .raw("</td></tr>");
    }
    h.raw("</tbody></table><nav class=\"pager\" id=\"search-next\">");
    if s.resumed {
        link(&mut h, &s.url(false, None), "« From the start");
        h.raw(" ");
    }
    match &page.next {
        Some(next) => link(&mut h, &s.url(false, Some(next)), "Next page of results »"),
        None => {
            h.raw("<span>End of the search.</span>");
        }
    }
    h.raw("</nav><p class=\"meta\">");
    link(&mut h, &s.url(true, Some(&s.cursor)), "JSON");
    h.raw("</p>");
    Ok(layout(app, &format!("Search {}", s.q), &crumbs, &h))
}

// --- counts ---------------------------------------------------------------------------------------

/// What is missing from a finished pass: `k key-groups unreadable, m malformed`.
fn incomplete_text(snap: &CountSnapshot) -> String {
    let mut parts = Vec::new();
    if snap.unreadable_key_groups > 0 {
        parts.push(format!(
            "{} key-group(s) unreadable",
            snap.unreadable_key_groups
        ));
    }
    if snap.malformed_key_groups > 0 {
        parts.push(format!("{} malformed", snap.malformed_key_groups));
    }
    parts.join(", ")
}

/// `N` when exact; `≥ N (incomplete: k key-groups unreadable)` when the pass ended without
/// reading some key-groups (whole); `≥ N (counting… x %)` while it runs, `≥ N (cancelled)` and
/// so on.
pub(super) fn count_text(snap: &CountSnapshot, n: u64) -> String {
    match &snap.status {
        JobStatus::Done if snap.exact() => n.to_string(),
        JobStatus::Done => format!("≥ {} (incomplete: {})", n, incomplete_text(snap)),
        JobStatus::Running => format!("≥ {} (counting… {:.0} %)", n, snap.progress.percent()),
        JobStatus::Cancelling => format!("≥ {} (cancelling…)", n),
        JobStatus::Cancelled => format!("≥ {} (cancelled at {:.0} %)", n, snap.progress.percent()),
        JobStatus::Failed(_) => format!("≥ {} (failed)", n),
    }
}

fn status_text(snap: &CountSnapshot) -> String {
    let p = progress_text(&snap.progress);
    let secs = snap.elapsed.as_secs_f64();
    match &snap.status {
        JobStatus::Running => format!("Counting… {} · {:.1} s", p, secs),
        JobStatus::Cancelling => format!("Cancelling… (stopped after {}).", p),
        JobStatus::Done if snap.exact() => format!(
            "Counted every key-group in {:.1} s ({} bytes read).",
            secs, snap.bytes_read
        ),
        JobStatus::Done => format!(
            "Incomplete: went through every key-group in {:.1} s ({} bytes read), but {}: \
             the counts are lower bounds.",
            secs,
            snap.bytes_read,
            incomplete_text(snap)
        ),
        JobStatus::Cancelled => format!("Cancelled after {}.", p),
        JobStatus::Failed(e) => format!("Failed after {}: {}", p, e),
    }
}

/// The counts of state `name` in `snap` (zero when the job has not met it yet).
fn state_counts(snap: &CountSnapshot, name: &str) -> (u64, u64) {
    snap.states
        .iter()
        .find(|c| &*c.state == name)
        .map(|c| (c.keys, c.entries))
        .unwrap_or((0, 0))
}

/// Start, show, cancel or restart the job of `op`, as the query says; without `side_effects`
/// (a `HEAD`), only show it (`None`: there is none).
fn count_job(
    app: &App,
    op: &str,
    q: &Query,
    side_effects: bool,
) -> Res<(String, Vec<String>, Option<CountSnapshot>)> {
    let (_, id) = keyed(app, op)?;
    let states = app.reader.states(&id)?;
    let names: Vec<String> = states.states.into_iter().map(|s| s.name).collect();
    let flag = |n: &str| q.get(n).is_some_and(|v| v == "1");
    let snap = if !side_effects {
        app.counts.get(&id)
    } else if flag("cancel") {
        Some(
            app.counts
                .cancel(&id)
                .ok_or_else(|| Fail::NotFound(format!("no count of operator {} to cancel", id)))?,
        )
    } else {
        let started = app
            .counts
            .start(&app.reader, &id, flag("restart"), &app.options);
        Some(started.map_err(|Busy(m)| Fail::Busy(m))?)
    };
    Ok((id, names, snap))
}

pub(super) fn count_json(id: &str, names: &[String], snap: &CountSnapshot) -> Value {
    // Every registered state, then any the blocks hold that no readable header named.
    let mut rows: Vec<Value> = names
        .iter()
        .map(|n| {
            let (k, e) = state_counts(snap, n);
            json!({
                "state": n, "keys": k, "entries": e,
                "keys_text": count_text(snap, k), "entries_text": count_text(snap, e),
            })
        })
        .collect();
    for c in &snap.states {
        if !names.iter().any(|n| **n == *c.state) {
            rows.push(json!({
                "state": &*c.state, "keys": c.keys, "entries": c.entries,
                "keys_text": count_text(snap, c.keys), "entries_text": count_text(snap, c.entries),
            }));
        }
    }
    json!({
        "operator": id,
        "status": snap.status.as_str(),
        "error": match &snap.status { JobStatus::Failed(e) => Some(e.as_str()), _ => None },
        "exact": snap.exact(),
        "unreadable_key_groups": snap.unreadable_key_groups,
        "malformed_key_groups": snap.malformed_key_groups,
        "keys": snap.keys,
        "keys_text": count_text(snap, snap.keys),
        "states": rows,
        "progress": progress_json(&snap.progress),
        "status_text": status_text(snap),
        "elapsed_ms": snap.elapsed.as_millis() as u64,
        "bytes_read": snap.bytes_read,
        "unreadable": unreadable_json(&snap.unreadable),
        "malformed": snap.malformed,
        "more_warnings": snap.more_warnings,
    })
}

pub(super) fn api_count(app: &App, op: &str, q: &Query, side_effects: bool) -> Res<Response> {
    let (id, names, snap) = count_job(app, op, q, side_effects)?;
    let v = match snap {
        Some(snap) => count_json(&id, &names, &snap),
        None => json!({ "operator": id, "status": "none" }),
    };
    Ok(json_response(200, &v))
}

/// The cells of one state's counts, as the operator and count pages show them; `app.js` updates
/// the ones under a `[data-count-api]` element, matching rows by `data-state`.
pub(super) fn count_cells(h: &mut Html, snap: Option<&CountSnapshot>, name: &str) {
    match snap {
        Some(snap) => {
            let (k, e) = state_counts(snap, name);
            h.raw("<td class=\"count-keys\">")
                .text(&count_text(snap, k))
                .raw("</td><td class=\"count-entries\">")
                .text(&count_text(snap, e))
                .raw("</td>");
        }
        None => {
            h.raw("<td class=\"count-keys\">—</td><td class=\"count-entries\">—</td>");
        }
    }
}

pub(super) fn count_page(app: &App, op: &str, q: &Query, side_effects: bool) -> Res<Response> {
    let (id, names, snap) = count_job(app, op, q, side_effects)?;
    // A HEAD and no job: nothing to render (the body is dropped anyway).
    let Some(snap) = snap else {
        return Ok(Response::new(200, "text/html; charset=utf-8", Vec::new()));
    };
    let api = format!("/api{}", op_path(&id, "count"));
    let mut h = Html::new();
    h.raw("<h1>Key counts</h1><div id=\"counts\"");
    if snap.status.is_live() {
        h.raw(" data-count-api=\"").text(&api).raw("\"");
    }
    h.raw("><p id=\"count-status\" class=\"meta\">")
        .text(&status_text(&snap))
        .raw("</p><p id=\"count-actions\">");
    match snap.status {
        JobStatus::Running => link(
            &mut h,
            &href(&op_path(&id, "count"), &[("cancel", "1")]),
            "Cancel",
        ),
        // Not restartable until its worker has stopped.
        JobStatus::Cancelling => {
            h.raw("<span>Cancelling…</span>");
        }
        _ => link(
            &mut h,
            &href(&op_path(&id, "count"), &[("restart", "1")]),
            "Count again",
        ),
    }
    h.raw(" · ");
    link(&mut h, &op_path(&id, "count"), "Refresh");
    h.raw("</p>");
    unreadable_warnings(&mut h, &snap.unreadable);
    malformed_warnings(&mut h, &snap.malformed);
    h.raw("<table><thead><tr><th>State</th><th>Keys</th><th>Entries (export rows)</th></tr>")
        .raw("</thead><tbody>");
    let row = |h: &mut Html, name: &str| {
        h.raw("<tr data-state=\"").text(name).raw("\"><td>");
        link(h, &href(&op_path(&id, "state"), &[("name", name)]), name);
        h.raw("</td>");
        count_cells(h, Some(&snap), name);
        h.raw("</tr>");
    };
    for n in &names {
        row(&mut h, n);
    }
    for c in &snap.states {
        if !names.iter().any(|n| **n == *c.state) {
            row(&mut h, &c.state);
        }
    }
    h.raw("</tbody><tfoot><tr><th>Distinct keys</th><td id=\"count-total\">")
        .text(&count_text(&snap, snap.keys))
        .raw("</td><td></td></tr></tfoot></table>");
    h.raw(
        "<p class=\"note\">Entries are what a download of the state holds rows of: one per \
         value, per MAP entry, per timer. The pass reads every key-group once, a few megabytes \
         at a time, and keeps its results for as long as the server runs. ",
    );
    link(&mut h, &api, "JSON");
    h.raw("</p></div>");
    Ok(layout(
        app,
        "Key counts",
        &[op_crumb(&id), (String::new(), "counts".to_string())],
        &h,
    ))
}

// --- downloads ------------------------------------------------------------------------------------

/// `<operator id prefix>-<state>.<ext>`, the state reduced to `[A-Za-z0-9._-]` (anything else
/// becomes `_`, no leading dot or `..`, at most 100 characters): safe in a quoted
/// `Content-Disposition` filename on every OS.
pub fn download_filename(op_id: &str, state: &str, ext: &str) -> String {
    let mut name = String::new();
    for c in state.chars() {
        let c = if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
            c
        } else {
            '_'
        };
        // No leading dot (a hidden file), no `..`.
        if c == '.' && (name.is_empty() || name.ends_with('.')) {
            continue;
        }
        name.push(c);
        if name.len() == 100 {
            break;
        }
    }
    if name.is_empty() {
        name.push_str("state");
    }
    let prefix: String = op_id
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(8)
        .collect();
    format!("{}-{}.{}", prefix, name, ext)
}

pub(super) fn download(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (_, id) = resolve(app, op)?;
    let state = q.required("state")?.to_string();
    check_state(app, &id, &state)?;
    let format = q
        .get("format")
        .filter(|f| !f.is_empty())
        .unwrap_or("ndjson");
    let (content_type, ext) = match format {
        "ndjson" => ("application/x-ndjson", "jsonl"),
        "parquet" if cfg!(feature = "parquet") => ("application/vnd.apache.parquet", "parquet"),
        "parquet" => {
            return Err(Fail::BadRequest(
                "this build has no parquet support; rebuild with `--features parquet`".into(),
            ))
        }
        _ => {
            return Err(Fail::BadRequest(
                "format must be \"ndjson\" or \"parquet\"".into(),
            ))
        }
    };
    let parquet = ext == "parquet";
    let reader = app.reader.clone();
    let op_id = id.clone();
    let name = state.clone();
    let produce: super::StreamFn = Box::new(move |out| {
        let stats = if parquet {
            parquet_rows(&reader, &op_id, &name, out)?
        } else {
            let mut sink = NdjsonSink::new(out);
            rows(&reader, &op_id, &name, &mut sink)?
        };
        // An incomplete download must not pass for a complete one: abort the transfer (the
        // browser marks it failed). `--export-state` writes the same rows and a warning.
        if stats.malformed_key_groups > 0 {
            return Err(format!(
                "{} malformed key-group(s); their entries past the defect are missing (first: {})",
                stats.malformed_key_groups,
                stats.first_malformed.as_deref().unwrap_or("?")
            ));
        }
        Ok(())
    });
    let mut r = Response::new(200, content_type, Vec::new());
    r.headers.push((
        "Content-Disposition",
        format!(
            "attachment; filename=\"{}\"",
            download_filename(&id, &state, ext)
        ),
    ));
    r.stream = Some(Streamed::new(produce));
    Ok(r)
}

fn rows(
    reader: &crate::reader::StateReader<'static>,
    op: &str,
    state: &str,
    sink: &mut dyn RowSink,
) -> Result<crate::export_state::ExportStats, String> {
    export_state_rows(
        reader.savepoint(),
        reader.savepoint_dir(),
        Some(op),
        state,
        reader.proto(),
        sink,
    )
}

#[cfg(feature = "parquet")]
fn parquet_rows(
    reader: &crate::reader::StateReader<'static>,
    op: &str,
    state: &str,
    out: &mut (dyn std::io::Write + Send),
) -> Result<crate::export_state::ExportStats, String> {
    let mut sink = crate::export_parquet::ParquetSink::new(out);
    rows(reader, op, state, &mut sink)
}

#[cfg(not(feature = "parquet"))]
fn parquet_rows(
    _reader: &crate::reader::StateReader<'static>,
    _op: &str,
    _state: &str,
    _out: &mut (dyn std::io::Write + Send),
) -> Result<crate::export_state::ExportStats, String> {
    Err("this build has no parquet support".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filenames_are_sanitised() {
        let id = "0123456789abcdef0123456789abcdef";
        assert_eq!(
            download_filename(id, "count", "jsonl"),
            "01234567-count.jsonl"
        );
        assert_eq!(
            download_filename(id, "_timer_state/event_user-timers", "jsonl"),
            "01234567-_timer_state_event_user-timers.jsonl"
        );
        let hostile = download_filename(id, "a\"; filename=x.exe\r\n../../é", "parquet");
        assert!(hostile
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)));
        assert!(hostile.ends_with(".parquet"));
        assert_eq!(download_filename(id, "..", "jsonl"), "01234567-state.jsonl");
        assert!(download_filename(id, &"x".repeat(500), "jsonl").len() <= 8 + 1 + 100 + 6);
    }
}
