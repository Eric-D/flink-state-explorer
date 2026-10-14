//! `--web` search, counts and downloads (#23), over synthetic savepoints, spoken to in raw
//! HTTP/1.1 (chunked bodies decoded here, so what is tested is the bytes on the wire).
//! Fixture-free: runs in CI.
#![cfg(feature = "web")]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use common::synth::{self, Item, KeyedOperator, Kind, RunOrder, Ser, Spec, Storage, Val};
use flink_explorer::export_state::{
    export_state_ndjson, export_state_rows, list_states, RowSink, StateRow,
};
use flink_explorer::parser;
use flink_explorer::parser::key_group::{java_string_hash, key_group_for_hash};
use flink_explorer::reader::StateReader;
use flink_explorer::web::{self, RunningServer, WebOptions};
use serde_json::Value;

const HOSTILE_KEY: &str = "<img src=x onerror=alert(1)>";
/// The opaque-key operator's one key, as the explorer renders it.
const OPAQUE_KEY: &str = "0x010203";

/// The sample operator (String keys: VALUE Long, VALUE String, MAP, LIST, timers) on two
/// subtasks, plus one hostile key, and an operator with opaque keys (`get` cannot hash them).
fn spec(keys: usize, storage: Storage, compressed: bool) -> (Spec, String, String) {
    let mut op = synth::sample_operator(keys);
    let hostile = Val::str(HOSTILE_KEY);
    op.items.push(Item::Value {
        state: 0,
        key: hostile.clone(),
        value: Val::Long(666),
    });
    op.items.push(Item::MapEntry {
        state: 2,
        key: hostile,
        map_key: Val::str("\"><svg onload=alert(2)>"),
        value: Some(Val::Long(1)),
    });
    let op_hex = op.id_hex();
    let mut opaque = KeyedOperator::new(
        0x51,
        1,
        128,
        Ser::Opaque("com.example.OpaqueKeySerializer$OpaqueKeySerializerSnapshot"),
    )
    .state("v", Kind::Value(Ser::Long));
    opaque.items.push(Item::Value {
        state: 0,
        key: Val::Opaque {
            bytes: vec![1, 2, 3],
            hash: 5,
        },
        value: Val::Long(1),
    });
    let opaque_hex = opaque.id_hex();
    let mut spec = Spec::new(storage);
    spec.compressed = compressed;
    spec.run_order = RunOrder::HashLike;
    spec.operators.push(op);
    spec.operators.push(opaque);
    (spec, op_hex, opaque_hex)
}

struct Fixture {
    _dir: synth::TempDir,
    dir: std::path::PathBuf,
    server: RunningServer,
    op: String,
    opaque: String,
    cookie: String,
}

fn fixture(keys: usize, storage: Storage, compressed: bool) -> Fixture {
    fixture_with(keys, storage, compressed, WebOptions::default(), |_| {})
}

fn fixture_with(
    keys: usize,
    storage: Storage,
    compressed: bool,
    options: WebOptions,
    damage: impl FnOnce(&synth::Written),
) -> Fixture {
    let (spec, op, opaque) = spec(keys, storage, compressed);
    let dir = synth::temp_dir("web-tools");
    let written = synth::write_savepoint(&spec, &dir);
    damage(&written);
    let reader = StateReader::open(&dir).expect("synthetic savepoint opens");
    let server = web::start_with_options(reader, "127.0.0.1:0".parse().unwrap(), options)
        .expect("server starts");
    let mut f = Fixture {
        dir: dir.to_path_buf(),
        _dir: dir,
        server,
        op,
        opaque,
        cookie: String::new(),
    };
    f.cookie = login(&f);
    f
}

/// Every key of [`spec`]'s main operator.
fn all_keys(keys: usize) -> Vec<String> {
    let mut v: Vec<String> = (0..keys).map(|i| format!("key-{:07}", i)).collect();
    v.push(HOSTILE_KEY.to_string());
    v
}

// --- raw HTTP/1.1 ---------------------------------------------------------------------------------

#[derive(Debug)]
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    /// A chunked body ended with its last chunk (not cut by an aborted connection).
    complete: bool,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("{}: {}", e, self.text()))
    }
}

/// `Transfer-Encoding: chunked` → the body, and whether the final chunk arrived.
fn dechunk(mut raw: &[u8]) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    loop {
        let Some(eol) = raw.windows(2).position(|w| w == b"\r\n") else {
            return (out, false);
        };
        let size_line = std::str::from_utf8(&raw[..eol]).unwrap();
        let size = usize::from_str_radix(size_line.split(';').next().unwrap().trim(), 16).unwrap();
        raw = &raw[eol + 2..];
        if size == 0 {
            return (out, true);
        }
        if raw.len() < size + 2 {
            out.extend_from_slice(&raw[..raw.len().min(size)]);
            return (out, false);
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
}

fn send(addr: SocketAddr, raw: &str) -> Resp {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    s.write_all(raw.as_bytes()).unwrap();
    let mut buf = Vec::new();
    // An aborted chunked body ends in a reset: keep what arrived.
    let _ = s.read_to_end(&mut buf);
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a header block");
    let head = String::from_utf8_lossy(&buf[..split]).into_owned();
    let rest = &buf[split + 4..];
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_string(), v.trim().to_string()))
        .collect();
    let chunked = headers.iter().any(|(n, v)| {
        n.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });
    let (body, complete) = if chunked {
        dechunk(rest)
    } else {
        (rest.to_vec(), true)
    };
    Resp {
        status,
        headers,
        body,
        complete,
    }
}

fn request(f: &Fixture, method: &str, target: &str, cookie: Option<&str>) -> Resp {
    let mut raw = format!(
        "{} {} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n",
        method,
        target,
        f.server.addr().port()
    );
    if let Some(c) = cookie {
        raw.push_str(&format!("Cookie: {}\r\n", c));
    }
    if method == "POST" {
        raw.push_str("Content-Length: 0\r\n");
    }
    raw.push_str("\r\n");
    send(f.server.addr(), &raw)
}

fn login(f: &Fixture) -> String {
    let token = f.server.url().split("token=").nth(1).unwrap().to_string();
    let r = request(f, "GET", &format!("/?token={}", token), None);
    assert_eq!(r.status, 200, "{:?}", r);
    let set = r.header("Set-Cookie").expect("Set-Cookie");
    set.split(';').next().unwrap().to_string()
}

fn get(f: &Fixture, target: &str) -> Resp {
    request(f, "GET", target, Some(&f.cookie))
}

fn enc(s: &str) -> String {
    web::html::url_encode(s)
}

fn unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

// --- search ---------------------------------------------------------------------------------------

/// Follow a JSON search from `first` through every `next_api`: the keys found (in order), and
/// how many requests it took.
fn search_all(f: &Fixture, first: &str) -> (Vec<String>, usize, Value) {
    let mut keys = Vec::new();
    let mut url = first.to_string();
    let mut requests = 0;
    loop {
        let r = get(f, &url);
        assert_eq!(r.status, 200, "{}: {}", url, r.text());
        assert_eq!(r.header("Content-Type"), Some("application/json"));
        let v = r.json();
        requests += 1;
        for m in v["matches"].as_array().unwrap() {
            keys.push(m["key"].as_str().unwrap().to_string());
            assert!(m["href"].as_str().unwrap().starts_with("/op/"));
        }
        assert!(requests < 10_000, "the search never ends");
        match v["next_api"].as_str() {
            Some(next) => {
                assert_eq!(v["done"], false);
                url = next.to_string();
            }
            None => {
                assert_eq!(v["done"], true);
                return (keys, requests, v);
            }
        }
    }
}

fn expect_once(found: &[String], want: &BTreeSet<String>, what: &str) {
    let set: BTreeSet<String> = found.iter().cloned().collect();
    assert_eq!(
        set.len(),
        found.len(),
        "{}: a key came twice: {:?}",
        what,
        found
    );
    assert_eq!(&set, want, "{}", what);
}

#[test]
fn search_across_continuations_returns_every_match_exactly_once() {
    for (storage, compressed) in [(Storage::Files, false), (Storage::Inline, true)] {
        let f = fixture(300, storage, compressed);
        let keys = all_keys(300);
        type Case = (&'static str, &'static str, fn(&str) -> bool);
        let cases: [Case; 4] = [
            ("glob", "KEY-00001*", |k: &str| k.starts_with("key-00001")),
            ("glob", "*-00000?7", |k: &str| {
                k.starts_with("key-00000") && k.ends_with('7')
            }),
            ("substring", "0002", |k: &str| k.contains("0002")),
            ("substring", "ONERROR=", |k: &str| k.contains("onerror=")),
        ];
        for (mode, q, pred) in &cases {
            let want: BTreeSet<String> = keys.iter().filter(|k| pred(k)).cloned().collect();
            assert!(!want.is_empty(), "{}", q);
            // A 1-byte budget and 3 matches per request: one block per request, and many cursors.
            let first = format!(
                "/api/op/{}/search?q={}&mode={}&budget=1&limit=3",
                f.op,
                enc(q),
                mode
            );
            let (found, requests, last) = search_all(&f, &first);
            expect_once(&found, &want, q);
            assert!(requests > 20, "{}: only {} requests", q, requests);
            let p = &last["progress"];
            assert_eq!(p["key_groups_done"], p["key_groups_total"], "{}", last);
            assert_eq!(p["bytes_done"], p["bytes_total"]);
            assert_eq!(p["key_groups_total"], 128);
            assert_eq!(p["percent"], 100.0);
            // A generous budget gives the same keys in one request.
            let (all_at_once, n, _) = search_all(
                &f,
                &format!(
                    "/api/op/{}/search?q={}&mode={}&limit=1000",
                    f.op,
                    enc(q),
                    mode
                ),
            );
            assert_eq!(n, 1);
            assert_eq!(all_at_once, found, "same order, whatever the budget");
        }
    }
}

/// Without JS: the HTML page's "Next page of results" link goes through the whole scan.
#[test]
fn the_search_page_works_as_next_page_links() {
    let f = fixture(200, Storage::Files, false);
    let want: BTreeSet<String> = all_keys(200)
        .into_iter()
        .filter(|k| k.contains("0001"))
        .collect();
    let mut url = format!("/op/{}/search?q=0001&mode=substring&budget=1&limit=2", f.op);
    let mut found = Vec::new();
    let mut pages = 0;
    loop {
        let r = get(&f, &url);
        assert_eq!(r.status, 200, "{}", r.text());
        let body = r.text();
        pages += 1;
        assert!(body.contains("id=\"search-status\""));
        for cell in body.split("<td class=\"key\"><a href=\"").skip(1) {
            let text = cell
                .split_once("\">")
                .unwrap()
                .1
                .split("</a>")
                .next()
                .unwrap();
            found.push(unescape(text));
        }
        let next = body
            .split("<a href=\"")
            .filter_map(|s| s.split_once("\">"))
            .find(|(_, text)| text.starts_with("Next page of results"))
            .map(|(href, _)| unescape(href));
        match next {
            Some(n) => {
                // The JS continuation is announced on the same element, for the same cursor.
                assert!(body.contains("data-api=\"/api/op/"), "{}", body);
                url = n;
            }
            None => {
                assert!(body.contains("End of the search."));
                assert!(!body.contains("data-api="));
                break;
            }
        }
    }
    assert!(pages > 10, "{}", pages);
    expect_once(&found, &want, "html");
}

#[test]
fn a_hostile_query_is_escaped_and_hostile_keys_are_inert() {
    let f = fixture(30, Storage::Inline, false);
    let q = "<script>alert(1)</script>\"'*";
    let r = get(&f, &format!("/op/{}/search?q={}", f.op, enc(q)));
    assert_eq!(r.status, 200);
    let body = r.text();
    assert!(!body.contains("<script>alert"), "{}", body);
    assert!(body.contains("&lt;script&gt;alert(1)&lt;/script&gt;&quot;&#39;*"));
    assert_eq!(body.matches("<script").count(), 1, "only the static app.js");

    let r = get(&f, &format!("/op/{}/search?q=onerror&mode=substring", f.op));
    let body = r.text();
    assert!(!body.contains(HOSTILE_KEY));
    assert!(
        body.contains("&lt;img src=x onerror=alert(1)&gt;"),
        "{}",
        body
    );
    assert!(!body.contains("<svg"));
    let v = get(
        &f,
        &format!("/api/op/{}/search?q=onerror&mode=substring", f.op),
    )
    .json();
    assert_eq!(v["matches"][0]["key"], HOSTILE_KEY);
}

#[test]
fn a_key_group_range_restricts_the_search() {
    let f = fixture(300, Storage::Files, false);
    // Across the two subtasks' boundary (64).
    for (from, to) in [(50u16, 90u16), (0, 0), (64, 64), (100, 127)] {
        let want: BTreeSet<String> = all_keys(300)
            .into_iter()
            .filter(|k| {
                let kg = key_group_for_hash(java_string_hash(k), 128) as u16;
                (from..=to).contains(&kg)
            })
            .collect();
        let (found, _, last) = search_all(
            &f,
            &format!(
                "/api/op/{}/search?q=*&from={}&to={}&budget=1&limit=5",
                f.op, from, to
            ),
        );
        expect_once(&found, &want, &format!("{}..{}", from, to));
        assert_eq!(last["progress"]["key_groups_total"], to - from + 1);
        assert_eq!(last["progress"]["percent"], 100.0);
    }
}

#[test]
fn search_parameters_are_bounded() {
    let f = fixture(10, Storage::Inline, false);
    let long = "x".repeat(web::MAX_QUERY_CHARS + 1);
    let ok = "x".repeat(web::MAX_QUERY_CHARS);
    for (target, status) in [
        (format!("/api/op/{}/search?q={}", f.op, long), 400),
        (format!("/api/op/{}/search?q={}", f.op, ok), 200),
        (format!("/api/op/{}/search?q=a&budget=0", f.op), 400),
        (
            format!(
                "/api/op/{}/search?q=a&budget={}",
                f.op,
                web::MAX_SEARCH_BUDGET + 1
            ),
            400,
        ),
        (format!("/api/op/{}/search?q=a&limit=1001", f.op), 400),
        (format!("/api/op/{}/search?q=a&mode=regex", f.op), 400),
        (format!("/api/op/{}/search?q=a&from=9&to=3", f.op), 400),
        (format!("/api/op/{}/search?q=a&to=128", f.op), 400),
        (
            format!("/api/op/{}/search?q=a&from=10&cursor=0.3.0", f.op),
            400,
        ),
        (format!("/api/op/{}/search?q=a&cursor=1.3.0", f.op), 400),
        (format!("/api/op/{}/search", f.op), 400),
        (format!("/op/{}/search?q={}", f.op, long), 400),
    ] {
        let r = get(&f, &target);
        assert_eq!(r.status, status, "{}: {}", target, r.text());
    }
    // The form alone, without a query.
    let r = get(&f, &format!("/op/{}/search", f.op));
    assert_eq!(r.status, 200);
    assert!(r.text().contains("name=\"q\""));
}

/// A key type `get` cannot locate is offered a one-click scan, which finds it.
#[test]
fn a_key_needing_a_scan_is_offered_one() {
    let f = fixture(10, Storage::Inline, false);
    let r = get(&f, &format!("/op/{}/lookup?key={}", f.opaque, OPAQUE_KEY));
    assert_eq!(r.status, 200);
    let body = r.text();
    let scan = body
        .split("<a href=\"")
        .filter_map(|s| s.split_once("\">"))
        .find(|(_, text)| text.starts_with("Scan the operator for this exact key"))
        .map(|(href, _)| unescape(href))
        .unwrap_or_else(|| panic!("no scan link in {}", body));
    assert!(scan.contains("mode=exact"), "{}", scan);
    let r = get(&f, &scan);
    assert_eq!(r.status, 200);
    assert!(r.text().contains(OPAQUE_KEY));
    let v = get(
        &f,
        &format!("/api/op/{}/lookup?key={}", f.opaque, OPAQUE_KEY),
    )
    .json();
    assert_eq!(v["result"], "needs_scan");
    let (found, _, _) = search_all(&f, v["scan"].as_str().unwrap());
    assert_eq!(found, vec![OPAQUE_KEY.to_string()]);
}

// --- counts ---------------------------------------------------------------------------------------

struct Collect(Vec<(String, String)>);

impl RowSink for Collect {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        self.0.push((row.state.to_string(), row.key.to_string()));
        Ok(())
    }
    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }
}

fn wait_done(f: &Fixture, op: &str) -> Value {
    let start = Instant::now();
    loop {
        let v = get(f, &format!("/api/op/{}/count", op)).json();
        if v["status"] != "running" {
            return v;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "{}", v);
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn counts_equal_the_export_row_counts() {
    for (storage, compressed) in [(Storage::Files, false), (Storage::Inline, true)] {
        let f = fixture(400, storage, compressed);
        let v = wait_done(&f, &f.op);
        assert_eq!(v["status"], "done", "{}", v);
        assert_eq!(v["exact"], true);
        assert_eq!(v["keys"], 401, "{}", v);
        assert_eq!(v["progress"]["percent"], 100.0);

        let sp = parser::parse_metadata(&f.dir.join("_metadata")).unwrap();
        let listing = list_states(&sp, &f.dir);
        let states: Vec<&String> = listing
            .states
            .iter()
            .filter(|(op, _, _)| *op == f.op)
            .map(|(_, s, _)| s)
            .collect();
        assert_eq!(states.len(), 5);
        let got: BTreeMap<String, (u64, u64)> = v["states"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    s["state"].as_str().unwrap().to_string(),
                    (s["keys"].as_u64().unwrap(), s["entries"].as_u64().unwrap()),
                )
            })
            .collect();
        for state in states {
            let mut sink = Collect(Vec::new());
            export_state_rows(&sp, &f.dir, Some(&f.op), state, None, &mut sink).unwrap();
            let rows = sink.0.len() as u64;
            let keys = sink.0.iter().map(|(_, k)| k).collect::<BTreeSet<_>>().len() as u64;
            assert_eq!(got[state.as_str()], (keys, rows), "{}", state);
            assert!(rows > 0);
        }
        // The pages show the exact numbers, not "≥".
        let r = get(&f, &format!("/op/{}", f.op));
        let body = r.text();
        assert!(!body.contains("≥"), "{}", body);
        assert!(!body.contains("data-count-api"));
        assert!(
            body.contains("<td class=\"count-entries\">401</td>"),
            "{}",
            body
        );
        let r = get(&f, &format!("/op/{}/count", f.op));
        assert!(r.text().contains("<td id=\"count-total\">401</td>"));
        // Asking again shows the same job.
        assert_eq!(
            get(&f, &format!("/api/op/{}/count", f.op)).json()["keys"],
            401
        );
    }
}

#[test]
fn a_count_shows_its_progress_and_can_be_cancelled() {
    let options = WebOptions {
        count_step_bytes: 1,
        count_step_pause: Duration::from_millis(30),
    };
    let f = fixture_with(300, Storage::Files, false, options, |_| {});
    // Nothing to cancel yet.
    assert_eq!(get(&f, &format!("/op/{}/count?cancel=1", f.op)).status, 404);
    // The operator page does not start a count.
    assert!(get(&f, &format!("/op/{}", f.op))
        .text()
        .contains("Count keys and entries"));

    let r = get(&f, &format!("/op/{}/count", f.op));
    assert_eq!(r.status, 200);
    let body = r.text();
    assert!(body.contains("data-count-api=\"/api/op/"), "{}", body);
    assert!(body.contains("counting…"), "{}", body);
    std::thread::sleep(Duration::from_millis(200));
    let v = get(&f, &format!("/api/op/{}/count", f.op)).json();
    assert_eq!(v["status"], "running", "{}", v);
    assert!(v["keys_text"].as_str().unwrap().starts_with("≥ "), "{}", v);
    let page = get(&f, &format!("/op/{}", f.op)).text();
    assert!(page.contains("(counting… "), "{}", page);
    assert!(page.contains("data-count-api"));

    let v = get(&f, &format!("/api/op/{}/count?cancel=1", f.op)).json();
    assert_eq!(v["status"], "cancelled", "{}", v);
    std::thread::sleep(Duration::from_millis(200));
    // It stays cancelled: polling does not restart it, and it is no longer counting.
    let a = get(&f, &format!("/api/op/{}/count", f.op)).json();
    std::thread::sleep(Duration::from_millis(200));
    let b = get(&f, &format!("/api/op/{}/count", f.op)).json();
    assert_eq!(a["status"], "cancelled");
    assert_eq!(a["keys"], b["keys"], "a cancelled job does not go on");
    assert!(a["progress"]["percent"].as_f64().unwrap() < 100.0);
    assert!(a["keys"].as_u64().unwrap() < 301);
    let r = get(&f, &format!("/op/{}/count", f.op));
    assert!(r.text().contains("restart=1"));
    assert!(!r.text().contains("data-count-api"));
    // Cancelling again is harmless.
    assert_eq!(
        get(&f, &format!("/api/op/{}/count?cancel=1", f.op)).json()["status"],
        "cancelled"
    );

    // Restarted, it counts from scratch, to the end.
    let v = get(&f, &format!("/api/op/{}/count?restart=1", f.op)).json();
    assert_eq!(v["status"], "running");
    // A restart of a running job does not start a second one.
    let again = get(&f, &format!("/api/op/{}/count?restart=1", f.op)).json();
    assert_eq!(again["status"], "running");
    assert!(again["elapsed_ms"].as_u64() >= v["elapsed_ms"].as_u64());
    let done = wait_done(&f, &f.op);
    assert_eq!(done["status"], "done");
    assert_eq!(done["keys"], 301);
}

// --- downloads ------------------------------------------------------------------------------------

#[test]
fn ndjson_downloads_equal_the_export_byte_for_byte() {
    for (storage, compressed) in [(Storage::Files, true), (Storage::Inline, false)] {
        let f = fixture(2500, storage, compressed);
        let sp = parser::parse_metadata(&f.dir.join("_metadata")).unwrap();
        let listing = list_states(&sp, &f.dir);
        let mut big = false;
        for (_, state, _) in listing.states.iter().filter(|(op, _, _)| *op == f.op) {
            let mut want = Vec::new();
            export_state_ndjson(&sp, &f.dir, Some(&f.op), state, None, &mut want).unwrap();
            let r = get(
                &f,
                &format!("/op/{}/download?state={}&format=ndjson", f.op, enc(state)),
            );
            assert_eq!(r.status, 200, "{}", r.text());
            assert!(r.complete, "{}: the body ended with its last chunk", state);
            assert_eq!(r.header("Content-Type"), Some("application/x-ndjson"));
            assert_eq!(r.header("Transfer-Encoding"), Some("chunked"));
            let disposition = r.header("Content-Disposition").unwrap();
            assert_eq!(
                disposition,
                format!(
                    "attachment; filename=\"{}\"",
                    web::download_filename(&f.op, state, "jsonl")
                )
            );
            assert!(disposition.starts_with(&format!("attachment; filename=\"{}-", &f.op[..8])));
            assert!(r.header("Content-Security-Policy").is_some());
            assert_eq!(r.header("X-Content-Type-Options"), Some("nosniff"));
            assert!(
                r.body == want,
                "{}: {} vs {} bytes",
                state,
                r.body.len(),
                want.len()
            );
            big |= want.len() > 4 * 64 * 1024;
        }
        assert!(big, "some download spans many chunks");
    }
}

#[test]
fn download_filenames_are_sanitised() {
    let f = fixture(5, Storage::Inline, false);
    let state = "_timer_state/processing_user-timers";
    let r = get(&f, &format!("/op/{}/download?state={}", f.op, enc(state)));
    assert_eq!(r.status, 200);
    assert_eq!(
        r.header("Content-Disposition"),
        Some(
            format!(
                "attachment; filename=\"{}-_timer_state_processing_user-timers.jsonl\"",
                &f.op[..8]
            )
            .as_str()
        )
    );
    for hostile in [
        "a\"; filename=evil.exe",
        "../../etc/passwd",
        "x\r\nSet-Cookie: a=b",
        "é/ü",
    ] {
        let name = web::download_filename(&f.op, hostile, "jsonl");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)),
            "{}",
            name
        );
        assert!(!name.contains(".."), "{}", name);
        assert!(name.ends_with(".jsonl"));
    }
    // Only states of the operator are served; the format is checked.
    assert_eq!(
        get(&f, &format!("/op/{}/download?state=nope", f.op)).status,
        404
    );
    assert_eq!(get(&f, &format!("/op/{}/download", f.op)).status, 400);
    assert_eq!(
        get(
            &f,
            &format!("/op/{}/download?state=counter&format=csv", f.op)
        )
        .status,
        400
    );
    // HEAD: the headers, no body, nothing exported.
    let r = request(
        &f,
        "HEAD",
        &format!("/op/{}/download?state=counter", f.op),
        Some(&f.cookie),
    );
    assert_eq!(r.status, 200);
    assert!(r.body.is_empty());
    assert!(r.header("Content-Disposition").is_some());
}

#[cfg(not(feature = "parquet"))]
#[test]
fn parquet_needs_the_feature() {
    let f = fixture(5, Storage::Inline, false);
    let r = get(
        &f,
        &format!("/op/{}/download?state=counter&format=parquet", f.op),
    );
    assert_eq!(r.status, 400);
    assert!(r.text().contains("--features parquet"));
    assert!(!get(&f, &format!("/op/{}", f.op))
        .text()
        .contains(">Parquet<"));
}

#[cfg(feature = "parquet")]
#[test]
fn parquet_downloads_equal_the_parquet_sink() {
    let f = fixture(800, Storage::Files, false);
    assert!(get(&f, &format!("/op/{}", f.op))
        .text()
        .contains(">Parquet<"));
    let sp = parser::parse_metadata(&f.dir.join("_metadata")).unwrap();
    for state in ["counter", "stats", "_timer_state/processing_user-timers"] {
        let mut want = Vec::new();
        {
            let mut sink = flink_explorer::export_parquet::ParquetSink::new(&mut want);
            export_state_rows(&sp, &f.dir, Some(&f.op), state, None, &mut sink).unwrap();
        }
        let r = get(
            &f,
            &format!("/op/{}/download?state={}&format=parquet", f.op, enc(state)),
        );
        assert_eq!(r.status, 200, "{}", r.text());
        assert!(r.complete);
        assert_eq!(
            r.header("Content-Type"),
            Some("application/vnd.apache.parquet")
        );
        assert!(r
            .header("Content-Disposition")
            .unwrap()
            .ends_with(".parquet\""));
        assert!(r.body.starts_with(b"PAR1") && r.body.ends_with(b"PAR1"));
        assert!(r.body == want, "{}", state);
    }
}

/// A download that fails before its first byte is a `500` naming the cause, never an empty or
/// truncated `200`.
#[test]
fn a_download_that_cannot_be_read_is_an_error() {
    let lost = std::cell::RefCell::new(String::new());
    let f = fixture_with(60, Storage::Files, false, WebOptions::default(), |w| {
        let (_, _, path) = w.state_files.iter().find(|(_, s, _)| *s == 1).unwrap();
        *lost.borrow_mut() = path.file_name().unwrap().to_string_lossy().into_owned();
        std::fs::remove_file(path).unwrap();
    });
    let lost = lost.into_inner();
    let r = get(&f, &format!("/op/{}/download?state=counter", f.op));
    assert_eq!(r.status, 500, "{}", r.text());
    assert!(r.text().contains(&lost), "{}", r.text());
    assert!(r.header("Content-Disposition").is_none());
    assert!(r.header("Content-Security-Policy").is_some());
    // A count reports the lost key-groups instead of hiding them.
    let v = wait_done(&f, &f.op);
    assert_eq!(v["status"], "done");
    assert!(
        v["unreadable"][0]["error"]
            .as_str()
            .unwrap()
            .contains(&lost),
        "{}",
        v
    );
}

// --- access ---------------------------------------------------------------------------------------

#[test]
fn the_new_endpoints_need_the_session() {
    let f = fixture(10, Storage::Inline, false);
    let paths = [
        format!("/op/{}/search?q=*", f.op),
        format!("/api/op/{}/search?q=*", f.op),
        format!("/op/{}/count", f.op),
        format!("/api/op/{}/count", f.op),
        format!("/api/op/{}/count?cancel=1", f.op),
        format!("/op/{}/download?state=counter", f.op),
        format!("/op/{}/download?state=counter&format=parquet", f.op),
    ];
    for path in &paths {
        for cookie in [None, Some(format!("{}=0", f.server.cookie_name()))] {
            let r = request(&f, "GET", path, cookie.as_deref());
            assert_eq!(r.status, 403, "{}", path);
            assert!(!r.text().contains("key-0"), "{}", path);
            assert!(r.header("Content-Disposition").is_none());
        }
        // Read-only: a POST is refused even with the session.
        let r = request(&f, "POST", path, Some(&f.cookie));
        assert_eq!(r.status, 405, "{}", path);
    }
    // No count was started by any of these.
    let v = get(&f, &format!("/api/op/{}", f.op)).json();
    assert!(v["counts"].is_null(), "{}", v);
}

// --- exact lookup ---------------------------------------------------------------------------------

/// Every key is found by the exact lookup, and an absent key whose key-group is empty is
/// answered without reading anything.
#[test]
fn every_key_is_found_by_lookup_and_an_empty_key_group_costs_nothing() {
    let f = fixture(40, Storage::Files, true);
    let keys = all_keys(40);
    for k in &keys {
        let v = get(&f, &format!("/api/op/{}/lookup?key={}", f.op, enc(k))).json();
        assert_eq!(v["result"], "found", "{}: {}", k, v);
        assert_eq!(v["record"]["key"], k.as_str());
    }
    let used: BTreeSet<i32> = keys
        .iter()
        .map(|k| key_group_for_hash(java_string_hash(k), 128))
        .collect();
    let absent = (0..)
        .map(|i| format!("absent-{}", i))
        .find(|k| !used.contains(&key_group_for_hash(java_string_hash(k), 128)))
        .unwrap();
    let v = get(&f, &format!("/api/op/{}/lookup?key={}", f.op, absent)).json();
    assert_eq!(v["result"], "absent", "{}", v);
    assert_eq!(v["blocks_read"], 0);
    let r = get(&f, &format!("/op/{}/lookup?key={}", f.op, absent));
    assert!(r.text().contains("nothing had to be read"));
}
