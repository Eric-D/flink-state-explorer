//! Routes: HTML pages and their `/api/` JSON twins, over [`crate::reader::StateReader`] only.
//!
//! Values are rendered from the `serde_json::Value` the reader decoded (through
//! [`crate::parser::value`]), with [`value_to_display`] for leaves: nothing here decodes bytes.
//! `/api/` rows are built by [`crate::export_state::row_json`], so they have exactly the shape of
//! an `--export-state` NDJSON line.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::html::{url_decode_path, url_encode, Html};
use super::{App, Response};
use crate::export_state::{namespace_hex, row_json, StateRow};
use crate::parser::state_handle::{StateHandle, StreamStateHandle};
use crate::parser::value::value_to_display;
use crate::reader::{
    hex_id, Cursor, EntryPage, KeyPage, KeyRecord, KeyedStates, Lookup, ReadStats, ReaderError,
    StateEntry, Unreadable,
};

const CSS: &str = include_str!("assets/app.css");
const JS: &str = include_str!("assets/app.js");

/// Keys per page unless `limit` says otherwise, and the most a page may ask for.
pub const DEFAULT_LIMIT: usize = 100;
pub const MAX_LIMIT: usize = 1000;
/// Array/object members shown in a value tree before "… N more".
const TREE_MEMBERS: usize = 1000;
/// Raw bytes shown in hex before the dump is cut.
const HEX_BYTES: usize = 4096;
/// Characters of a value shown in a table cell.
const CELL_CHARS: usize = 160;
/// Characters of the one-line preview next to a value tree's summary.
const PREVIEW_CHARS: usize = 80;
/// Entries per page of the entries view unless `limit` says otherwise.
const DEFAULT_ENTRY_PAGE: usize = 100;

/// The most entries of one state a key record carries in the web UI (the reader's per-record
/// cap, [`crate::reader::StateReader::with_entry_limit`]): a key page renders at most this many
/// per state and links to the paged entries view for the rest. `--web` opens its reader with it.
pub const ENTRY_LIMIT: usize = 200;

/// Why a request failed: the status, and a message safe to show (it is escaped like any text).
enum Fail {
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

type Res<T> = Result<T, Fail>;

/// The status a reader error deserves: the caller's fault (unknown operator, bad cursor, …) or
/// the savepoint's (unreadable, malformed).
impl From<ReaderError> for Fail {
    fn from(e: ReaderError) -> Fail {
        let msg = e.to_string();
        match e {
            ReaderError::UnknownOperator { .. }
            | ReaderError::NoKeyedState { .. }
            | ReaderError::NotFound { .. } => Fail::NotFound(msg),
            ReaderError::AmbiguousOperator { .. } | ReaderError::BadCursor { .. } => {
                Fail::BadRequest(msg)
            }
            ReaderError::Unreadable { .. }
            | ReaderError::Malformed { .. }
            | ReaderError::Metadata { .. } => Fail::Internal(msg),
        }
    }
}

pub(super) fn route(app: &App, path: &str, query: &[(String, String)]) -> Response {
    let segments: Vec<String> = path.split('/').skip(1).map(url_decode_path).collect();
    let segs: Vec<&str> = segments.iter().map(String::as_str).collect();
    let q = Query(query);
    let (api, result) = match segs.as_slice() {
        [""] => (false, overview(app)),
        ["assets", "app.css"] => {
            return Response::new(200, "text/css; charset=utf-8", CSS.as_bytes())
        }
        ["assets", "app.js"] => {
            return Response::new(200, "text/javascript; charset=utf-8", JS.as_bytes())
        }
        ["op", op] => (false, operator_page(app, op)),
        ["op", op, "keys"] => (false, keys_page(app, op, &q, None)),
        ["op", op, "state"] => (
            false,
            q.required("name")
                .and_then(|n| keys_page(app, op, &q, Some(n))),
        ),
        ["op", op, "key"] => (false, key_page(app, op, &q)),
        ["op", op, "lookup"] => (false, lookup_page(app, op, &q)),
        ["op", op, "entries"] => (false, entries_page(app, op, &q)),
        ["api", "operators"] => (true, api_operators(app)),
        ["api", "op", op] => (true, api_operator(app, op)),
        ["api", "op", op, "keys"] => (true, api_keys(app, op, &q, None)),
        ["api", "op", op, "state"] => (
            true,
            q.required("name")
                .and_then(|n| api_keys(app, op, &q, Some(n))),
        ),
        ["api", "op", op, "key"] => (true, api_key(app, op, &q)),
        ["api", "op", op, "lookup"] => (true, api_lookup(app, op, &q)),
        ["api", "op", op, "entries"] => (true, api_entries(app, op, &q)),
        _ if segs.first() == Some(&"api") => {
            (true, Err(Fail::NotFound("no such API route".into())))
        }
        _ => (false, Err(Fail::NotFound("no such page".into()))),
    };
    match result {
        Ok(r) => r,
        Err(fail) => {
            let (status, msg) = match fail {
                Fail::NotFound(m) => (404, m),
                Fail::BadRequest(m) => (400, m),
                Fail::Internal(m) => (500, m),
            };
            if api {
                json_response(status, &json!({ "error": msg }))
            } else {
                let mut body = Html::new();
                body.raw("<h1>")
                    .display(status)
                    .raw("</h1><p class=\"error\">");
                body.text(&msg)
                    .raw("</p><p><a href=\"/\">Back to the overview</a></p>");
                let mut r = layout(app, "Error", &[], &body);
                r.status = status;
                r
            }
        }
    }
}

// --- query parameters ---------------------------------------------------------------------------

struct Query<'a>(&'a [(String, String)]);

impl Query<'_> {
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn required(&self, name: &str) -> Res<&str> {
        self.get(name)
            .ok_or_else(|| Fail::BadRequest(format!("missing parameter {:?}", name)))
    }

    fn cursor(&self, name: &str) -> Res<Option<Cursor>> {
        match self.get(name) {
            None | Some("") => Ok(None),
            Some(c) => c.parse().map(Some).map_err(Fail::BadRequest),
        }
    }

    fn limit(&self) -> Res<usize> {
        self.limit_or(DEFAULT_LIMIT)
    }

    fn limit_or(&self, default: usize) -> Res<usize> {
        match self.get("limit") {
            None | Some("") => Ok(default),
            Some(l) => match l.parse::<usize>() {
                Ok(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
                _ => Err(Fail::BadRequest(format!(
                    "limit must be between 1 and {}",
                    MAX_LIMIT
                ))),
            },
        }
    }
}

// --- operator data (shared by the page and its JSON twin) ---------------------------------------

/// The canonical hex id of the operator `op` names (a hex id or a unique prefix of one).
fn resolve(app: &App, op: &str) -> Res<(usize, String)> {
    // An empty prefix would match every operator: `/op/` is not a page.
    if op.is_empty() {
        return Err(Fail::NotFound("no operator id given".into()));
    }
    let i = app.reader.operator_index(op)?;
    Ok((i, hex_id(&app.reader.savepoint().operators[i].id)))
}

/// One operator state, as `_metadata` registers it (element values are not read here).
struct OpStateInfo {
    name: String,
    mode: &'static str,
    subtasks: usize,
    elements: usize,
}

struct OperatorView {
    id: String,
    lines: Vec<(&'static str, String)>,
    keyed: Option<Result<KeyedStates, String>>,
    op_states: Vec<OpStateInfo>,
}

fn stream_size(h: &StreamStateHandle) -> u64 {
    match h {
        StreamStateHandle::ByteStream(b) => b.data.len() as u64,
        StreamStateHandle::File(f) => f.state_size.max(0) as u64,
        StreamStateHandle::Relative(r) => r.state_size.max(0) as u64,
        StreamStateHandle::Segment(s) => s.state_size.max(0) as u64,
        StreamStateHandle::KeyGroups(k) => stream_size(&k.delegate_handle),
        StreamStateHandle::Null | StreamStateHandle::EmptySegment => 0,
    }
}

fn mode_label(mode: i32) -> &'static str {
    match mode {
        0 => "SPLIT_DISTRIBUTE",
        1 => "UNION",
        2 => "BROADCAST",
        _ => "?",
    }
}

fn operator_view(app: &App, op_index: usize) -> OperatorView {
    let op = &app.reader.savepoint().operators[op_index];
    let id = hex_id(&op.id);
    let unknown = || "(not in this savepoint's metadata)".to_string();
    let mut keyed_size = 0u64;
    let mut op_size = 0u64;
    let mut keyed_subtasks = 0usize;
    let mut op_states: BTreeMap<String, OpStateInfo> = BTreeMap::new();
    for sub in &op.subtask_states {
        if let StateHandle::KeyGroups(k) = &sub.managed_keyed_state {
            keyed_size += stream_size(&k.delegate_handle);
            keyed_subtasks += 1;
        }
        if let Some(StateHandle::OperatorStream(os)) = &sub.managed_operator_state {
            op_size += stream_size(&os.delegate_handle);
            for p in &os.partition_offsets {
                let e = op_states
                    .entry(p.state_name.clone())
                    .or_insert_with(|| OpStateInfo {
                        name: p.state_name.clone(),
                        mode: mode_label(p.mode),
                        subtasks: 0,
                        elements: 0,
                    });
                e.subtasks += 1;
                e.elements += p.offsets.len();
            }
        }
    }
    let mut lines = vec![
        ("OperatorID", id.clone()),
        (
            "Name",
            if op.name.is_empty() {
                unknown()
            } else {
                op.name.clone()
            },
        ),
        (
            "uid / identifier",
            if op.identifier_name.is_empty() {
                unknown()
            } else {
                op.identifier_name.clone()
            },
        ),
        ("Parallelism", op.parallelism.to_string()),
        ("Max parallelism", op.max_parallelism.to_string()),
        ("Subtasks", op.subtask_states.len().to_string()),
        ("Subtasks with keyed state", keyed_subtasks.to_string()),
        ("Keyed state size", format!("{} bytes", keyed_size)),
        ("Operator state size", format!("{} bytes", op_size)),
        (
            "Coordinator state",
            if op.coordinator_state.is_some() {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("Fully finished", op.fully_finished.to_string()),
    ];
    if let Some(info) = op
        .coordinator_state
        .as_deref()
        .and_then(crate::parser::kafka::decode_enumerator_state)
    {
        lines.push(("Source enumerator", info));
    }
    let keyed = (keyed_subtasks > 0).then(|| app.reader.states(&id).map_err(|e| e.to_string()));
    if let Some(Ok(k)) = &keyed {
        lines.push(("Key type", k.key_type.clone()));
    }
    OperatorView {
        id,
        lines,
        keyed,
        op_states: op_states.into_values().collect(),
    }
}

fn keyed_size(app: &App, op_index: usize) -> u64 {
    app.reader.savepoint().operators[op_index]
        .subtask_states
        .iter()
        .map(|s| match &s.managed_keyed_state {
            StateHandle::KeyGroups(k) => stream_size(&k.delegate_handle),
            _ => 0,
        })
        .sum()
}

// --- links ----------------------------------------------------------------------------------------

fn href(path: &str, params: &[(&str, &str)]) -> String {
    let mut s = path.to_string();
    for (i, (k, v)) in params.iter().enumerate() {
        s.push(if i == 0 { '?' } else { '&' });
        s.push_str(&url_encode(k));
        s.push('=');
        s.push_str(&url_encode(v));
    }
    s
}

fn op_path(op: &str, sub: &str) -> String {
    if sub.is_empty() {
        format!("/op/{}", url_encode(op))
    } else {
        format!("/op/{}/{}", url_encode(op), sub)
    }
}

fn link(h: &mut Html, href: &str, text: &str) {
    h.raw("<a href=\"")
        .text(href)
        .raw("\">")
        .text(text)
        .raw("</a>");
}

fn clip(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

// --- layout ---------------------------------------------------------------------------------------

fn layout(app: &App, title: &str, crumbs: &[(String, String)], body: &Html) -> Response {
    let mut h = Html::new();
    h.raw("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">")
        .raw("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
        .raw("<meta name=\"referrer\" content=\"no-referrer\"><title>")
        .text(title)
        .raw(" · flink-explorer</title>")
        .raw("<link rel=\"stylesheet\" href=\"/assets/app.css\">")
        .raw("<script src=\"/assets/app.js\" defer></script></head><body>")
        .raw("<header><nav class=\"crumbs\"><a href=\"/\">")
        .text(&app.title)
        .raw("</a>");
    for (href, text) in crumbs {
        h.raw(" <span>›</span> ");
        if href.is_empty() {
            h.raw("<span>").text(text).raw("</span>");
        } else {
            link(&mut h, href, text);
        }
    }
    h.raw("</nav><span class=\"ro\">read-only</span></header><main>")
        .append(body)
        .raw("</main></body></html>\n");
    Response::new(
        200,
        "text/html; charset=utf-8",
        h.into_string().into_bytes(),
    )
}

fn json_response(status: u16, v: &Value) -> Response {
    let body = serde_json::to_vec_pretty(v).unwrap_or_else(|_| b"{}".to_vec());
    Response::new(status, "application/json", body)
}

fn op_crumb(id: &str) -> (String, String) {
    (
        op_path(id, ""),
        format!("operator {}", &id[..id.len().min(12)]),
    )
}

fn lookup_form(h: &mut Html, id: &str, value: &str) {
    h.raw("<form class=\"lookup\" method=\"get\" action=\"")
        .text(&op_path(id, "lookup"))
        .raw("\"><label>Look a key up <input type=\"search\" name=\"key\" id=\"lookup-key\" value=\"")
        .text(value)
        .raw("\" placeholder=\"exact key, as rendered\" required></label> ")
        .raw("<button type=\"submit\">Find</button></form>");
}

// --- overview -------------------------------------------------------------------------------------

fn overview(app: &App) -> Res<Response> {
    let sp = app.reader.savepoint();
    let mut h = Html::new();
    h.raw("<h1>Savepoint ").text(&app.title).raw("</h1>");
    h.raw("<p class=\"meta\">Checkpoint ")
        .display(sp.checkpoint_id)
        .raw(" · metadata v")
        .display(sp.version)
        .raw(" · ")
        .display(sp.operators.len())
        .raw(" operators · ")
        .text(&app.reader.savepoint_dir().display().to_string())
        .raw("</p>");
    h.raw("<table><thead><tr><th>Operator</th><th>Name</th><th>Parallelism</th>")
        .raw(
            "<th>Keyed state</th><th>Keyed states</th><th>Operator states</th></tr></thead><tbody>",
        );
    for (i, info) in app.reader.operators().iter().enumerate() {
        let view = operator_view(app, i);
        h.raw("<tr><td class=\"mono\">");
        link(&mut h, &op_path(&info.id, ""), &info.id);
        // Names and uids are in metadata v5+ only.
        let name = if info.name.is_empty() {
            "—"
        } else {
            &info.name
        };
        h.raw("</td><td>").text(name).raw("</td><td>");
        h.display(info.parallelism)
            .raw(" / ")
            .display(info.max_parallelism)
            .raw("</td><td>");
        if info.keyed_subtasks > 0 {
            h.display(keyed_size(app, i))
                .raw(" bytes, ")
                .display(info.keyed_subtasks)
                .raw(" subtask(s)");
        } else {
            h.raw("—");
        }
        h.raw("</td><td>");
        match &view.keyed {
            None => {
                h.raw("—");
            }
            Some(Err(e)) => {
                h.raw("<span class=\"error\">").text(e).raw("</span>");
            }
            Some(Ok(k)) => {
                for (j, s) in k.states.iter().enumerate() {
                    if j > 0 {
                        h.raw(", ");
                    }
                    link(
                        &mut h,
                        &href(&op_path(&info.id, "state"), &[("name", &s.name)]),
                        &s.name,
                    );
                }
                if !k.errors.is_empty() {
                    h.raw(" <span class=\"error\">(")
                        .display(k.errors.len())
                        .raw(" unreadable file(s))</span>");
                }
            }
        }
        h.raw("</td><td>");
        if view.op_states.is_empty() {
            h.raw("—");
        }
        for (j, s) in view.op_states.iter().enumerate() {
            if j > 0 {
                h.raw(", ");
            }
            h.text(&s.name);
        }
        h.raw("</td></tr>");
    }
    h.raw("</tbody></table>");
    Ok(layout(app, &app.title, &[], &h))
}

fn api_operators(app: &App) -> Res<Response> {
    let sp = app.reader.savepoint();
    let ops: Vec<Value> = app
        .reader
        .operators()
        .iter()
        .enumerate()
        .map(|(i, o)| {
            json!({
                "id": o.id,
                "name": o.name,
                "parallelism": o.parallelism,
                "max_parallelism": o.max_parallelism,
                "keyed_subtasks": o.keyed_subtasks,
                "keyed_state_bytes": keyed_size(app, i),
            })
        })
        .collect();
    Ok(json_response(
        200,
        &json!({
            "checkpoint_id": sp.checkpoint_id,
            "metadata_version": sp.version,
            "operators": ops,
        }),
    ))
}

// --- operator -------------------------------------------------------------------------------------

fn operator_page(app: &App, op: &str) -> Res<Response> {
    let (i, id) = resolve(app, op)?;
    let view = operator_view(app, i);
    let mut h = Html::new();
    h.raw("<h1>Operator <span class=\"mono\">")
        .text(&id)
        .raw("</span></h1>");
    h.raw("<table class=\"info\"><tbody>");
    for (label, value) in &view.lines {
        h.raw("<tr><th>")
            .text(label)
            .raw("</th><td>")
            .text(value)
            .raw("</td></tr>");
    }
    h.raw("</tbody></table>");

    match &view.keyed {
        None => {
            h.raw("<p>This operator holds no keyed state.</p>");
        }
        Some(Err(e)) => {
            h.raw("<p class=\"error\">").text(e).raw("</p>");
        }
        Some(Ok(k)) => {
            h.raw("<h2>Keyed state</h2>");
            lookup_form(&mut h, &id, "");
            h.raw("<p>");
            link(
                &mut h,
                &op_path(&id, "keys"),
                "Browse every key with all its states",
            );
            h.raw("</p>");
            for e in &k.errors {
                h.raw("<p class=\"error\">").text(e).raw("</p>");
            }
            h.raw("<table><thead><tr><th>State</th><th>Type</th><th>Value type</th>")
                .raw("<th>Value class</th><th>Subtasks</th></tr></thead><tbody>");
            for s in &k.states {
                h.raw("<tr><td>");
                link(
                    &mut h,
                    &href(&op_path(&id, "state"), &[("name", &s.name)]),
                    &s.name,
                );
                h.raw("</td><td>")
                    .text(&s.state_type)
                    .raw("</td><td>")
                    .text(&s.value_type)
                    .raw("</td><td class=\"mono\">")
                    .text(s.value_class.as_deref().unwrap_or(""))
                    .raw("</td><td>")
                    .display(s.subtasks)
                    .raw("</td></tr>");
            }
            h.raw("</tbody></table>");
        }
    }

    if !view.op_states.is_empty() {
        h.raw("<h2>Operator state</h2><table><thead><tr><th>State</th><th>Mode</th>")
            .raw("<th>Subtasks</th><th>Elements</th></tr></thead><tbody>");
        for s in &view.op_states {
            h.raw("<tr><td>")
                .text(&s.name)
                .raw("</td><td>")
                .text(s.mode)
                .raw("</td><td>")
                .display(s.subtasks)
                .raw("</td><td>")
                .display(s.elements)
                .raw("</td></tr>");
        }
        h.raw("</tbody></table><p class=\"note\">Operator-state element values are not shown \
               here: open this savepoint in the TUI (<code>flink-explorer &lt;savepoint&gt;</code>) \
               to see them decoded.</p>");
    }
    Ok(layout(
        app,
        &format!("Operator {}", id),
        &[op_crumb(&id)],
        &h,
    ))
}

fn api_operator(app: &App, op: &str) -> Res<Response> {
    let (i, _) = resolve(app, op)?;
    let view = operator_view(app, i);
    let info: Vec<Value> = view
        .lines
        .iter()
        .map(|(l, v)| json!({ "label": l, "value": v }))
        .collect();
    let op_states: Vec<Value> = view
        .op_states
        .iter()
        .map(|s| {
            json!({ "name": s.name, "mode": s.mode, "subtasks": s.subtasks, "elements": s.elements })
        })
        .collect();
    let keyed = match &view.keyed {
        None => Value::Null,
        Some(Err(e)) => json!({ "error": e }),
        Some(Ok(k)) => json!({
            "key_type": k.key_type,
            "states": k.states.iter().map(|s| json!({
                "name": s.name,
                "state_type": s.state_type,
                "value_type": s.value_type,
                "value_class": s.value_class,
                "subtasks": s.subtasks,
            })).collect::<Vec<_>>(),
            "errors": k.errors,
        }),
    };
    Ok(json_response(
        200,
        &json!({ "id": view.id, "info": info, "keyed": keyed, "operator_states": op_states }),
    ))
}

// --- key pages ------------------------------------------------------------------------------------

/// Check that `name` is a keyed state of the operator.
fn check_state(app: &App, id: &str, name: &str) -> Res<()> {
    let states = app.reader.states(id)?;
    if states.states.iter().any(|s| s.name == name) {
        Ok(())
    } else {
        Err(Fail::NotFound(format!(
            "operator {} has no keyed state {:?}",
            id, name
        )))
    }
}

fn read_page(
    app: &App,
    op: &str,
    q: &Query,
    state: Option<&str>,
) -> Res<(String, Cursor, usize, KeyPage)> {
    let (_, id) = resolve(app, op)?;
    if let Some(name) = state {
        check_state(app, &id, name)?;
    }
    let cursor = q.cursor("cursor")?.unwrap_or(Cursor::START);
    let limit = q.limit()?;
    let page = app.reader.key_page(&id, &cursor, limit)?;
    Ok((id, cursor, limit, page))
}

fn entry_row(op: &str, rec: &KeyRecord, e: &StateEntry) -> Value {
    entry_row_at(op, rec.subtask, &rec.key, rec.key_group, e)
}

fn entry_row_at(op: &str, subtask: i32, key: &str, key_group: u16, e: &StateEntry) -> Value {
    let ns = namespace_hex(&e.namespace, e.map_key.is_some());
    row_json(&StateRow {
        operator: op,
        subtask,
        state: &e.state,
        state_type: &e.state_type,
        value_type: &e.value_type,
        key,
        key_group,
        map_key: e.map_key.as_deref(),
        namespace_hex: ns.as_deref(),
        value: &e.value,
    })
}

fn stats_json(stats: &ReadStats) -> Value {
    json!({
        "blocks": stats.blocks,
        "cache_hits": stats.cache_hits,
        "bytes_read": stats.bytes_read,
        "block_bytes": stats.block_bytes,
        "max_block_bytes": stats.max_block_bytes,
    })
}

fn unreadable_json(list: &[Unreadable]) -> Value {
    Value::Array(
        list.iter()
            .map(|u| {
                json!({
                    "subtask": u.subtask,
                    "first_key_group": u.first_key_group,
                    "last_key_group": u.last_key_group,
                    "what": u.what,
                    "error": u.error,
                })
            })
            .collect(),
    )
}

/// The states of `rec` cut at the entry limit, with where to read the rest.
fn truncated_json(id: &str, rec: &KeyRecord) -> Value {
    json!({
        "key": rec.key,
        "position": rec.position.to_string(),
        "states": rec.truncated.iter().map(|t| json!({
            "state": &*t.state,
            "total": t.total,
            "returned": t.returned,
            "more": entries_href(true, id, &rec.position, &t.state, t.returned),
        })).collect::<Vec<_>>(),
    })
}

/// The entries view of one state of the key at `position`, from `offset`.
fn entries_href(api: bool, id: &str, position: &Cursor, state: &str, offset: usize) -> String {
    let base = op_path(id, "entries");
    let base = if api { format!("/api{}", base) } else { base };
    href(
        &base,
        &[
            ("at", &position.to_string()),
            ("state", state),
            ("offset", &offset.to_string()),
        ],
    )
}

/// A visible warning listing each key-group range a page could not read (and skipped).
fn unreadable_warnings(h: &mut Html, list: &[Unreadable]) {
    if list.is_empty() {
        return;
    }
    h.raw(
        "<div class=\"warning\" role=\"alert\"><strong>Some key-groups could not be read and \
         were skipped: their keys are missing from this page.</strong><ul>",
    );
    for u in list {
        h.raw("<li>Subtask ").display(u.subtask).raw(", key-group");
        if u.first_key_group == u.last_key_group {
            h.raw(" ").display(u.first_key_group);
        } else {
            h.raw("s ")
                .display(u.first_key_group)
                .raw("–")
                .display(u.last_key_group);
        }
        h.raw(" (<span class=\"mono\">")
            .text(&u.what)
            .raw("</span>): <span class=\"error\">")
            .text(&u.error)
            .raw("</span></li>");
    }
    h.raw("</ul></div>");
}

fn malformed_warnings(h: &mut Html, list: &[String]) {
    for m in list {
        h.raw(
            "<p class=\"error\" role=\"alert\">Malformed key-group (what follows the defect \
             could not be read): ",
        )
        .text(m)
        .raw("</p>");
    }
}

fn api_keys(app: &App, op: &str, q: &Query, state: Option<&str>) -> Res<Response> {
    let (id, _, _, page) = read_page(app, op, q, state)?;
    let rows: Vec<Value> = page
        .keys
        .iter()
        .flat_map(|rec| {
            rec.entries
                .iter()
                .filter(|e| state.is_none_or(|s| &*e.state == s))
                .map(|e| entry_row(&id, rec, e))
                .collect::<Vec<_>>()
        })
        .collect();
    Ok(json_response(
        200,
        &json!({
            "operator": id,
            "state": state,
            "keys": page.keys.len(),
            "rows": rows,
            "next": page.next.map(|c| c.to_string()),
            "malformed": page.malformed,
            "unreadable": unreadable_json(&page.unreadable),
            "truncated": page
                .keys
                .iter()
                .filter(|r| r.is_truncated())
                .map(|r| truncated_json(&id, r))
                .collect::<Vec<_>>(),
            "stats": stats_json(&page.stats),
        }),
    ))
}

fn keys_page(app: &App, op: &str, q: &Query, state: Option<&str>) -> Res<Response> {
    let (id, cursor, limit, page) = read_page(app, op, q, state)?;
    let mut h = Html::new();
    let base = match state {
        Some(_) => op_path(&id, "state"),
        None => op_path(&id, "keys"),
    };
    let params = |c: Option<&str>| {
        let mut p: Vec<(&str, String)> = Vec::new();
        if let Some(s) = state {
            p.push(("name", s.to_string()));
        }
        if let Some(c) = c {
            p.push(("cursor", c.to_string()));
        }
        if limit != DEFAULT_LIMIT {
            p.push(("limit", limit.to_string()));
        }
        p
    };
    let page_href = |c: Option<&str>| {
        let p = params(c);
        let refs: Vec<(&str, &str)> = p.iter().map(|(k, v)| (*k, v.as_str())).collect();
        href(&base, &refs)
    };

    match state {
        Some(s) => h.raw("<h1>State ").text(s).raw("</h1>"),
        None => h.raw("<h1>Keys</h1>"),
    };
    lookup_form(&mut h, &id, "");
    h.raw(
        "<p class=\"note\">Keys in on-disk order: key-group by key-group, then as they appear \
           in each key-group's block. Counts and search come later.</p>",
    );
    unreadable_warnings(&mut h, &page.unreadable);
    malformed_warnings(&mut h, &page.malformed);
    h.raw("<table class=\"keys\"><thead><tr><th>Key</th>");
    if state.is_some() {
        h.raw("<th>Map key</th><th>Value</th>");
    } else {
        h.raw("<th>States</th>");
    }
    h.raw("<th>Key-group</th><th>Subtask</th></tr></thead><tbody>");
    let mut rows = 0usize;
    for rec in &page.keys {
        let key_href = href(&op_path(&id, "key"), &[("at", &rec.position.to_string())]);
        match state {
            Some(s) => {
                for e in rec.entries.iter().filter(|e| &*e.state == s) {
                    rows += 1;
                    h.raw("<tr><td class=\"key\">");
                    link(&mut h, &key_href, &rec.key);
                    h.raw("</td><td>")
                        .text(e.map_key.as_deref().unwrap_or(""))
                        .raw("</td><td class=\"val\">")
                        .text(&preview(&e.value, CELL_CHARS))
                        .raw("</td>");
                    kg_cells(&mut h, rec);
                }
                // The record stops at the entry limit: the rest is one link away.
                if let Some(t) = rec.truncated.iter().find(|t| &*t.state == s) {
                    h.raw("<tr><td class=\"key\">");
                    link(&mut h, &key_href, &rec.key);
                    h.raw("</td><td colspan=\"2\" class=\"more\">… ")
                        .display(t.total - t.returned)
                        .raw(" more entries of this key: ");
                    link(
                        &mut h,
                        &entries_href(false, &id, &rec.position, s, t.returned),
                        "More entries »",
                    );
                    h.raw("</td>");
                    kg_cells(&mut h, rec);
                }
            }
            None => {
                rows += 1;
                h.raw("<tr><td class=\"key\">");
                link(&mut h, &key_href, &rec.key);
                h.raw("</td><td class=\"val\">");
                h.text(&summary(rec, CELL_CHARS));
                if rec.is_truncated() {
                    h.raw(" <span class=\"note\">(more entries on the key's page)</span>");
                }
                h.raw("</td>");
                kg_cells(&mut h, rec);
            }
        }
    }
    h.raw("</tbody></table>");
    if rows == 0 {
        h.raw("<p>No ").raw(if state.is_some() {
            "entries of this state"
        } else {
            "keys"
        });
        h.raw(" on this page.</p>");
    }
    h.raw("<nav class=\"pager\">");
    if cursor != Cursor::START {
        link(&mut h, &page_href(None), "« First page");
        h.raw(" ");
    }
    match page.next {
        Some(next) => link(&mut h, &page_href(Some(&next.to_string())), "Next page »"),
        None => {
            h.raw("<span>End of the keys.</span>");
        }
    }
    h.raw("</nav><p class=\"meta\">")
        .display(page.keys.len())
        .raw(" key(s) from ")
        .display(cursor)
        .raw(" · ")
        .display(page.stats.blocks)
        .raw(" block(s), ")
        .display(page.stats.bytes_read)
        .raw(" bytes read · ");
    let api = format!("/api{}", page_href(Some(&cursor.to_string())));
    link(&mut h, &api, "JSON");
    h.raw("</p>");
    let title = match state {
        Some(s) => format!("State {}", s),
        None => "Keys".to_string(),
    };
    let crumb = (String::new(), title.clone());
    Ok(layout(app, &title, &[op_crumb(&id), crumb], &h))
}

fn kg_cells(h: &mut Html, rec: &KeyRecord) {
    h.raw("<td>")
        .display(rec.key_group)
        .raw("</td><td>")
        .display(rec.subtask)
        .raw("</td></tr>");
}

// --- one key --------------------------------------------------------------------------------------

/// The key at `at`, exactly: a page of one from there, which must start with that very key.
fn key_at(app: &App, op: &str, q: &Query) -> Res<(String, KeyRecord, Vec<String>)> {
    let (_, id) = resolve(app, op)?;
    let at = q
        .cursor("at")?
        .ok_or_else(|| Fail::BadRequest("missing parameter \"at\"".into()))?;
    let page = app.reader.key_page(&id, &at, 1)?;
    let malformed = page.malformed;
    match page.keys.into_iter().next() {
        Some(rec) if rec.position == at => Ok((id, rec, malformed)),
        _ => match page.unreadable.into_iter().next() {
            // The key-group at `at` could not be read: say why rather than "no key".
            Some(u) if u.first_key_group <= at.key_group && at.key_group <= u.last_key_group => {
                Err(Fail::Internal(u.error))
            }
            _ => Err(Fail::NotFound(format!("no key at position {}", at))),
        },
    }
}

fn key_json(id: &str, rec: &KeyRecord, malformed: &[String]) -> Value {
    json!({
        "key": rec.key,
        "key_group": rec.key_group,
        "subtask": rec.subtask,
        "position": rec.position.to_string(),
        "rows": rec.entries.iter().map(|e| entry_row(id, rec, e)).collect::<Vec<_>>(),
        "raw": rec.entries.iter().map(raw_json).collect::<Vec<_>>(),
        "truncated": truncated_json(id, rec)["states"].clone(),
        "malformed": malformed,
    })
}

fn raw_json(e: &StateEntry) -> Value {
    json!({
        "state": &*e.state,
        "namespace_hex": hex(&e.namespace),
        "value_hex": hex(&e.raw),
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn api_key(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, rec, malformed) = key_at(app, op, q)?;
    Ok(json_response(200, &key_json(&id, &rec, &malformed)))
}

fn key_page(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, rec, malformed) = key_at(app, op, q)?;
    let mut h = Html::new();
    render_key(&mut h, &id, &rec, &malformed);
    let title = format!("Key {}", clip(&rec.key, 60));
    Ok(layout(
        app,
        &title,
        &[op_crumb(&id), (String::new(), title.clone())],
        &h,
    ))
}

/// One key. Renders the record's entries only, which the reader caps per state
/// ([`ENTRY_LIMIT`] under `--web`); a state cut there links to the paged entries view.
fn render_key(h: &mut Html, id: &str, rec: &KeyRecord, malformed: &[String]) {
    h.raw("<h1>Key <span class=\"key\">")
        .text(&rec.key)
        .raw("</span></h1>");
    h.raw("<p class=\"meta\">Key-group ")
        .display(rec.key_group)
        .raw(" · subtask ")
        .display(rec.subtask)
        .raw(" · position ")
        .display(rec.position)
        .raw(" · ")
        .display(rec.entries.len())
        .raw(" entr")
        .raw(if rec.entries.len() == 1 { "y" } else { "ies" })
        .raw(" shown · ");
    link(
        h,
        &href(
            &format!("/api{}", op_path(id, "key")),
            &[("at", &rec.position.to_string())],
        ),
        "JSON",
    );
    h.raw("</p>");
    malformed_warnings(h, malformed);
    for t in &rec.truncated {
        h.raw("<p class=\"more\">State <strong>")
            .text(&t.state)
            .raw("</strong>: the first ")
            .display(t.returned)
            .raw(" of ")
            .display(t.total)
            .raw(" entries are shown here. ");
        link(
            h,
            &entries_href(false, id, &rec.position, &t.state, t.returned),
            "More entries »",
        );
        h.raw("</p>");
    }
    h.raw("<div class=\"tree-tools\"></div>");
    for e in &rec.entries {
        render_entry(h, e);
    }
}

fn render_entry(h: &mut Html, e: &StateEntry) {
    h.raw("<section class=\"entry\"><h2>").text(&e.state);
    if let Some(mk) = &e.map_key {
        h.raw(" <span class=\"mapkey\">[").text(mk).raw("]</span>");
    }
    h.raw(" <small>")
        .text(&e.state_type)
        .raw(" · ")
        .text(&e.value_type)
        .raw("</small></h2><div class=\"tree\">");
    value_tree(h, &e.value, 0);
    h.raw("</div><details class=\"raw\"><summary>Raw bytes (")
        .display(e.raw.len())
        .raw(" value, ")
        .display(e.namespace.len())
        .raw(" namespace)</summary><pre>");
    if !e.namespace.is_empty() {
        h.raw("namespace\n");
        hex_dump(h, &e.namespace);
        h.raw("\nvalue\n");
    }
    hex_dump(h, &e.raw);
    h.raw("</pre></details></section>");
}

/// A `<details>` tree of a decoded value; leaves are rendered by [`value_to_display`].
fn value_tree(h: &mut Html, v: &Value, depth: usize) {
    let len = match v {
        Value::Object(m) if !m.is_empty() => m.len(),
        Value::Array(a) if !a.is_empty() => a.len(),
        leaf => {
            h.raw("<span class=\"v\">")
                .text(&value_to_display(leaf))
                .raw("</span>");
            return;
        }
    };
    h.raw(if depth < 2 {
        "<details open>"
    } else {
        "<details>"
    });
    h.raw("<summary>");
    if v.is_object() {
        h.raw("{ ").display(len).raw(" field(s) }");
    } else {
        h.raw("[ ").display(len).raw(" item(s) ]");
    }
    h.raw(" <span class=\"preview\">")
        .text(&preview(v, PREVIEW_CHARS))
        .raw("</span></summary><ul>");
    let mut member = |k: &str, child: &Value| {
        h.raw("<li><span class=\"k\">").text(k).raw("</span>: ");
        value_tree(h, child, depth + 1);
        h.raw("</li>");
    };
    match v {
        Value::Object(m) => m
            .iter()
            .take(TREE_MEMBERS)
            .for_each(|(k, child)| member(k, child)),
        Value::Array(a) => a
            .iter()
            .take(TREE_MEMBERS)
            .enumerate()
            .for_each(|(i, child)| member(&i.to_string(), child)),
        _ => {}
    }
    if len > TREE_MEMBERS {
        h.raw("<li>… ")
            .display(len - TREE_MEMBERS)
            .raw(" more (see the JSON)</li>");
    }
    h.raw("</ul></details>");
}

// --- previews -------------------------------------------------------------------------------------

/// The text [`value_to_display`] would give, cut at `max` characters (then `…`) **while it is
/// built**: a large value costs `max` characters, not a display string of the whole subtree.
fn preview(v: &Value, max: usize) -> String {
    let mut p = Preview::new(max);
    p.value(v);
    p.finish()
}

/// `state = value · state[map key] = value · …` for a row of the keys page, cut at `max`
/// characters while it is built.
fn summary(rec: &KeyRecord, max: usize) -> String {
    let mut p = Preview::new(max);
    for (i, e) in rec.entries.iter().enumerate() {
        let more = (i == 0 || p.push(" · "))
            && p.push(&e.state)
            && match &e.map_key {
                Some(mk) => p.push("[") && p.push(mk) && p.push("]"),
                None => true,
            }
            && p.push(" = ")
            && p.value(&e.value);
        if !more {
            break;
        }
    }
    p.finish()
}

/// A string that takes at most `left` more characters; every method returns `false` once it is
/// full, so that callers stop walking.
struct Preview {
    out: String,
    left: usize,
    cut: bool,
}

impl Preview {
    fn new(max: usize) -> Self {
        Preview {
            out: String::new(),
            left: max,
            cut: false,
        }
    }

    fn push(&mut self, s: &str) -> bool {
        if self.cut {
            return false;
        }
        for c in s.chars() {
            if self.left == 0 {
                self.cut = true;
                return false;
            }
            self.out.push(c);
            self.left -= 1;
        }
        true
    }

    /// The same text as [`value_to_display`], member by member.
    fn value(&mut self, v: &Value) -> bool {
        match v {
            Value::Null => self.push("null"),
            Value::Bool(b) => self.push(if *b { "true" } else { "false" }),
            Value::Number(n) => self.push(&n.to_string()),
            Value::String(s) if crate::parser::value::is_marker(s) => self.push(s),
            Value::String(s) => self.push("\"") && self.push(s) && self.push("\""),
            Value::Array(items) => {
                self.push("[")
                    && items
                        .iter()
                        .enumerate()
                        .all(|(i, x)| (i == 0 || self.push(", ")) && self.value(x))
                    && self.push("]")
            }
            Value::Object(map) => {
                self.push("{ ")
                    && map.iter().enumerate().all(|(i, (k, x))| {
                        (i == 0 || self.push(", "))
                            && self.push(k)
                            && self.push(": ")
                            && self.value(x)
                    })
                    && self.push(" }")
            }
        }
    }

    fn finish(mut self) -> String {
        if self.cut {
            self.out.push('…');
        }
        self.out
    }
}

// --- entries of one state of one key --------------------------------------------------------------

fn read_entries(app: &App, op: &str, q: &Query) -> Res<(String, Cursor, usize, EntryPage)> {
    let (_, id) = resolve(app, op)?;
    let at = q
        .cursor("at")?
        .ok_or_else(|| Fail::BadRequest("missing parameter \"at\"".into()))?;
    let state = q.required("state")?;
    let offset = match q.get("offset") {
        None | Some("") => 0,
        Some(o) => o
            .parse::<usize>()
            .map_err(|_| Fail::BadRequest("offset must be a non-negative integer".into()))?,
    };
    let limit = q.limit_or(DEFAULT_ENTRY_PAGE)?;
    let page = app.reader.entries(&id, &at, state, offset, limit)?;
    Ok((id, at, limit, page))
}

fn api_entries(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, at, _, page) = read_entries(app, op, q)?;
    let rows: Vec<Value> = page
        .entries
        .iter()
        .map(|e| entry_row_at(&id, at.subtask, &page.key, at.key_group, e))
        .collect();
    Ok(json_response(
        200,
        &json!({
            "operator": id,
            "key": page.key,
            "position": at.to_string(),
            "state": &*page.state,
            "offset": page.offset,
            "total": page.total,
            "rows": rows,
            "raw": page.entries.iter().map(raw_json).collect::<Vec<_>>(),
            "next": page.next,
            "malformed": page.malformed,
            "stats": stats_json(&page.stats),
        }),
    ))
}

fn entries_page(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, at, limit, page) = read_entries(app, op, q)?;
    let key_href = href(&op_path(&id, "key"), &[("at", &at.to_string())]);
    let mut h = Html::new();
    h.raw("<h1>State ")
        .text(&page.state)
        .raw(" of key <span class=\"key\">")
        .text(&page.key)
        .raw("</span></h1><p class=\"meta\">");
    if page.entries.is_empty() {
        h.raw("No entries from offset ").display(page.offset);
    } else {
        h.raw("Entries ")
            .display(page.offset + 1)
            .raw("–")
            .display(page.offset + page.entries.len());
    }
    h.raw(" of ").display(page.total).raw(" · ");
    link(&mut h, &key_href, "the whole key");
    h.raw(" · ");
    let page_href = |api: bool, offset: usize| {
        let mut s = entries_href(api, &id, &at, &page.state, offset);
        if limit != DEFAULT_ENTRY_PAGE {
            s.push_str(&format!("&limit={}", limit));
        }
        s
    };
    link(&mut h, &page_href(true, page.offset), "JSON");
    h.raw("</p>");
    malformed_warnings(&mut h, &page.malformed);
    h.raw("<div class=\"tree-tools\"></div>");
    for e in &page.entries {
        render_entry(&mut h, e);
    }
    h.raw("<nav class=\"pager\">");
    if page.offset > 0 {
        link(&mut h, &page_href(false, 0), "« First");
        h.raw(" ");
        link(
            &mut h,
            &page_href(false, page.offset.saturating_sub(limit)),
            "‹ Previous",
        );
        h.raw(" ");
    }
    match page.next {
        Some(next) => link(&mut h, &page_href(false, next), "Next »"),
        None => {
            h.raw("<span>End of this state's entries.</span>");
        }
    }
    h.raw("</nav>");
    let title = format!("{} of {}", &*page.state, clip(&page.key, 60));
    Ok(layout(
        app,
        &title,
        &[
            op_crumb(&id),
            (key_href.clone(), format!("Key {}", clip(&page.key, 60))),
            (String::new(), page.state.to_string()),
        ],
        &h,
    ))
}

fn hex_dump(h: &mut Html, bytes: &[u8]) {
    let shown = &bytes[..bytes.len().min(HEX_BYTES)];
    let mut s = String::new();
    for (i, chunk) in shown.chunks(16).enumerate() {
        s.push_str(&format!("{:08x}  ", i * 16));
        for j in 0..16 {
            match chunk.get(j) {
                Some(b) => s.push_str(&format!("{:02x} ", b)),
                None => s.push_str("   "),
            }
        }
        s.push(' ');
        s.extend(chunk.iter().map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        }));
        s.push('\n');
    }
    if bytes.len() > HEX_BYTES {
        s.push_str(&format!("… {} more bytes\n", bytes.len() - HEX_BYTES));
    }
    if bytes.is_empty() {
        s.push_str("(empty)\n");
    }
    h.text(&s);
}

// --- lookup ---------------------------------------------------------------------------------------

fn do_lookup(app: &App, op: &str, q: &Query) -> Res<(String, String, Lookup)> {
    let (_, id) = resolve(app, op)?;
    let key = q.required("key")?.to_string();
    let found = app.reader.get(&id, &key)?;
    Ok((id, key, found))
}

fn api_lookup(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, key, found) = do_lookup(app, op, q)?;
    let v = match found {
        Lookup::Found {
            record, malformed, ..
        } => {
            json!({
                "result": "found",
                "key": key,
                "record": key_json(&id, &record, &malformed),
            })
        }
        Lookup::Absent {
            key_group,
            subtask,
            stats,
        } => json!({
            "result": "absent",
            "key": key,
            "key_group": key_group,
            "subtask": subtask,
            "blocks_read": stats.blocks,
        }),
        Lookup::NeedsScan { reason } => {
            json!({ "result": "needs_scan", "key": key, "reason": reason })
        }
    };
    Ok(json_response(200, &v))
}

fn lookup_page(app: &App, op: &str, q: &Query) -> Res<Response> {
    let (id, key, found) = do_lookup(app, op, q)?;
    let mut h = Html::new();
    lookup_form(&mut h, &id, &key);
    match found {
        Lookup::Found {
            record, malformed, ..
        } => render_key(&mut h, &id, &record, &malformed),
        Lookup::Absent {
            key_group,
            subtask,
            stats,
        } => {
            h.raw("<h1>Not found</h1><p>No key <span class=\"key\">")
                .text(&key)
                .raw("</span> in this savepoint. Flink assigns it to key-group ")
                .display(key_group)
                .raw(", held by subtask ")
                .display(subtask)
                .raw(if stats.blocks == 0 {
                    ", which is empty (nothing had to be read).</p>"
                } else {
                    "; that key-group was read and does not hold it.</p>"
                });
        }
        Lookup::NeedsScan { reason } => {
            h.raw("<h1>Cannot look this key up directly</h1><p>")
                .text(&reason)
                .raw(
                    "</p><p>Its key-group cannot be computed from the text (POJO, Kryo or \
                      opaque keys have a user-defined <code>hashCode()</code>). ",
                );
            link(&mut h, &op_path(&id, "keys"), "Page through the keys");
            h.raw(" instead; search by scanning comes later.</p>");
        }
    }
    Ok(layout(
        app,
        &format!("Lookup {}", clip(&key, 60)),
        &[op_crumb(&id), (String::new(), "lookup".to_string())],
        &h,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_is_value_to_display_cut_while_built() {
        let v = json!({
            "a": 1, "b": [true, null, "x"], "c": {}, "d": "[Kryo]", "e": 1.5, "f": []
        });
        let full = value_to_display(&v);
        assert_eq!(preview(&v, 10_000), full);
        for max in [0, 1, 5, 17, full.chars().count() - 1] {
            let p = preview(&v, max);
            let head: String = full.chars().take(max).collect();
            assert_eq!(p, format!("{}…", head), "max {}", max);
        }
        // A huge value costs the budget, not its size.
        let big = Value::Array((0..1_000_000).map(|i| json!(i)).collect());
        assert_eq!(preview(&big, 8).chars().count(), 9);
    }
}
