//! `--web`: the server started in-process on `127.0.0.1:0` over synthetic savepoints
//! (`tests/common/synth.rs`), spoken to in raw HTTP/1.1 over `std::net::TcpStream` — no client
//! crate, so what is tested is exactly the bytes on the wire. Fixture-free: runs in CI.
#![cfg(feature = "web")]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use common::synth::{self, Item, KeyedOperator, OpState, RunOrder, Ser, Spec, Storage, Val};
use flink_explorer::export_state::{export_state_rows, list_states, row_json, RowSink, StateRow};
use flink_explorer::parser;
use flink_explorer::parser::key_group::{java_string_hash, key_group_for_hash};
use flink_explorer::reader::StateReader;
use flink_explorer::web::{self, RunningServer};
use serde_json::Value;

const HOSTILE_KEY: &str = "<img src=x onerror=alert(1)>";
const HOSTILE_VALUE: &str = "</script><script>alert(1)</script>";
const HOSTILE_MAP_KEY: &str = "\"><svg onload=alert(2)>";

const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; \
                   connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

/// The sample operator (String keys: VALUE Long, VALUE String, MAP, LIST, timers) on two
/// subtasks, plus one hostile key, and a non-keyed operator with operator state.
fn spec(keys: usize, storage: Storage, compressed: bool) -> (Spec, String) {
    let mut op = synth::sample_operator(keys);
    let hostile = Val::str(HOSTILE_KEY);
    op.items.push(Item::Value {
        state: 0,
        key: hostile.clone(),
        value: Val::Long(666),
    });
    op.items.push(Item::Value {
        state: 1,
        key: hostile.clone(),
        value: Val::str(HOSTILE_VALUE),
    });
    op.items.push(Item::MapEntry {
        state: 2,
        key: hostile,
        map_key: Val::str(HOSTILE_MAP_KEY),
        value: Some(Val::Long(1)),
    });
    let op_hex = op.id_hex();
    let source = KeyedOperator::new(0x51, 1, 128, Ser::String).op_state(
        0,
        OpState::list("offsets", Ser::String, vec![Val::str("a"), Val::str("b")]),
    );
    let mut spec = Spec::new(storage);
    spec.compressed = compressed;
    spec.run_order = RunOrder::HashLike;
    spec.operators.push(op);
    spec.operators.push(source);
    (spec, op_hex)
}

struct Fixture {
    _dir: synth::TempDir,
    dir: std::path::PathBuf,
    server: RunningServer,
    op: String,
}

fn fixture(keys: usize, storage: Storage, compressed: bool) -> Fixture {
    fixture_with(keys, storage, compressed, |_| {}, None)
}

/// [`fixture`], with `damage` applied to the written savepoint before the server opens it, and
/// the reader's per-record entry limit set to `entry_limit`.
fn fixture_with(
    keys: usize,
    storage: Storage,
    compressed: bool,
    damage: impl FnOnce(&synth::Written),
    entry_limit: Option<usize>,
) -> Fixture {
    let (spec, op) = spec(keys, storage, compressed);
    let dir = synth::temp_dir("web");
    let written = synth::write_savepoint(&spec, &dir);
    damage(&written);
    let mut reader = StateReader::open(&dir).expect("synthetic savepoint opens");
    if let Some(n) = entry_limit {
        reader = reader.with_entry_limit(n);
    }
    let server = web::start(reader, "127.0.0.1:0".parse().unwrap()).expect("server starts");
    Fixture {
        dir: dir.to_path_buf(),
        _dir: dir,
        server,
        op,
    }
}

// --- raw HTTP/1.1 ---------------------------------------------------------------------------------

#[derive(Debug)]
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{}: {}", e, self.body))
    }
}

/// Send `raw` (a whole request, `Connection: close`), read the whole response.
fn send(addr: SocketAddr, raw: &str) -> Resp {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    s.write_all(raw.as_bytes()).unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).expect("read response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").expect("a header block");
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap();
    let status = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("status line {:?}", status_line));
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_string(), v.trim().to_string()))
        .collect();
    Resp {
        status,
        headers,
        body: body.to_string(),
    }
}

fn request(addr: SocketAddr, method: &str, target: &str, host: &str, cookie: Option<&str>) -> Resp {
    let mut raw = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        method, target, host
    );
    if let Some(c) = cookie {
        raw.push_str(&format!("Cookie: {}\r\n", c));
    }
    if method == "POST" || method == "PUT" {
        raw.push_str("Content-Length: 0\r\n");
    }
    raw.push_str("\r\n");
    send(addr, &raw)
}

fn host(f: &Fixture) -> String {
    format!("127.0.0.1:{}", f.server.addr().port())
}

fn token(f: &Fixture) -> String {
    f.server.url().split("token=").nth(1).unwrap().to_string()
}

/// Exchange the token for the session cookie, as a browser does; returns `name=value`.
fn login(f: &Fixture) -> String {
    let r = request(
        f.server.addr(),
        "GET",
        &format!("/?token={}", token(f)),
        &host(f),
        None,
    );
    assert_eq!(r.status, 200, "{:?}", r);
    let set = r.header("Set-Cookie").expect("Set-Cookie");
    set.split(';').next().unwrap().to_string()
}

fn get(f: &Fixture, cookie: &str, target: &str) -> Resp {
    request(f.server.addr(), "GET", target, &host(f), Some(cookie))
}

fn enc(s: &str) -> String {
    web::html::url_encode(s)
}

fn assert_security_headers(r: &Resp) {
    assert_eq!(
        r.header("Content-Security-Policy"),
        Some(CSP),
        "{:?}",
        r.headers
    );
    assert_eq!(r.header("X-Content-Type-Options"), Some("nosniff"));
    assert_eq!(r.header("Referrer-Policy"), Some("no-referrer"));
    assert_eq!(r.header("Cache-Control"), Some("no-store"));
    assert_eq!(
        r.header("Cross-Origin-Resource-Policy"),
        Some("same-origin")
    );
    assert_eq!(r.header("Cross-Origin-Opener-Policy"), Some("same-origin"));
}

/// Where the login page sends the browser: its `meta refresh` target, which its link repeats.
fn refresh_target(r: &Resp) -> String {
    let content = r
        .body
        .split("<meta http-equiv=\"refresh\" content=\"0;url=")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("no meta refresh in {}", r.body))
        .replace("&amp;", "&");
    assert!(
        r.body
            .contains(&format!("<a href=\"{}\">", content.replace('&', "&amp;"))),
        "{}",
        r.body
    );
    content
}

// --- access control -------------------------------------------------------------------------------

#[test]
fn without_a_session_nothing_is_served() {
    let f = fixture(20, Storage::Inline, false);
    let paths = [
        "/".to_string(),
        "/api/operators".to_string(),
        format!("/op/{}", f.op),
        format!("/op/{}/keys", f.op),
        format!("/api/op/{}/keys", f.op),
        format!("/op/{}/lookup?key=key-0000001", f.op),
        "/assets/app.js".to_string(),
    ];
    let bad_cookies = [
        None,
        Some(format!(
            "{}=00000000000000000000000000000000",
            f.server.cookie_name()
        )),
        Some(format!("{}={}", "flink_explorer_1", token(&f))), // another port's name
        Some(format!("{}={}x", f.server.cookie_name(), token(&f))),
    ];
    for path in &paths {
        for cookie in &bad_cookies {
            let r = request(f.server.addr(), "GET", path, &host(&f), cookie.as_deref());
            assert_eq!(r.status, 403, "{} {:?}", path, cookie);
            assert_security_headers(&r);
            assert!(!r.body.contains("key-0000"), "{}: {}", path, r.body);
            assert!(!r.body.contains(&f.op), "{}: {}", path, r.body);
        }
    }
    // A wrong token is refused, and no cookie is handed out.
    let r = request(
        f.server.addr(),
        "GET",
        "/?token=0123456789abcdef0123456789abcdef",
        &host(&f),
        None,
    );
    assert_eq!(r.status, 403);
    assert!(r.header("Set-Cookie").is_none());
}

#[test]
fn the_token_is_exchanged_for_a_strict_cookie_and_a_clean_url() {
    let f = fixture(20, Storage::Inline, false);
    assert!(f.server.url().starts_with(&format!(
        "http://127.0.0.1:{}/?token=",
        f.server.addr().port()
    )));
    assert_eq!(token(&f).len(), 32, "128 bits in hex");

    let r = request(
        f.server.addr(),
        "GET",
        &format!("/?token={}", token(&f)),
        &host(&f),
        None,
    );
    // A 200 page that sets the cookie and moves on by itself, not a 303: a token link opened from
    // another site is a cross-site navigation, whose redirect may not carry a strict cookie.
    assert_eq!(r.status, 200);
    assert!(r.header("Location").is_none());
    assert_eq!(r.header("Content-Type"), Some("text/html; charset=utf-8"));
    assert_eq!(refresh_target(&r), "/");
    assert!(!r.body.contains(&token(&f)), "the token is not in the page");
    let set = r.header("Set-Cookie").unwrap();
    assert!(
        set.starts_with(&format!("{}={};", f.server.cookie_name(), token(&f))),
        "{}",
        set
    );
    for attr in ["HttpOnly", "SameSite=Strict", "Path=/"] {
        assert!(set.split("; ").any(|a| a == attr), "{} lacks {}", set, attr);
    }
    assert_security_headers(&r);

    // A deep link keeps its path and other parameters, minus the token.
    let r = request(
        f.server.addr(),
        "GET",
        &format!("/op/{}/keys?token={}&limit=5", f.op, token(&f)),
        &host(&f),
        None,
    );
    assert_eq!(r.status, 200);
    assert_eq!(refresh_target(&r), format!("/op/{}/keys?limit=5", f.op));
    // Never anywhere else: `//host` would be another origin.
    let r = request(
        f.server.addr(),
        "GET",
        &format!("//evil.example/?token={}", token(&f)),
        &host(&f),
        None,
    );
    assert_eq!(r.status, 200);
    assert_eq!(refresh_target(&r), "/");

    // The cookie opens the pages.
    let cookie = login(&f);
    let r = get(&f, &cookie, "/");
    assert_eq!(r.status, 200);
    assert!(r.body.contains(&f.op));
}

#[test]
fn a_foreign_host_header_is_rejected() {
    let f = fixture(20, Storage::Inline, false);
    let cookie = login(&f);
    let port = f.server.addr().port();
    for bad in [
        format!("evil.example:{}", port),
        "evil.example".to_string(),
        format!("localhost.evil.example:{}", port),
        format!("192.168.1.77:{}", port),
    ] {
        let r = request(f.server.addr(), "GET", "/", &bad, Some(&cookie));
        assert_eq!(r.status, 403, "{}", bad);
        assert!(!r.body.contains(&f.op), "{}", bad);
        assert_security_headers(&r);
    }
    // Even the token exchange: a rebinding page must not get a cookie.
    let r = request(
        f.server.addr(),
        "GET",
        &format!("/?token={}", token(&f)),
        &format!("evil.example:{}", port),
        None,
    );
    assert_eq!(r.status, 403);
    assert!(r.header("Set-Cookie").is_none());
    // An absolute-form target naming another host does not pass either.
    let r = send(
        f.server.addr(),
        &format!(
            "GET http://evil.example/ HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nCookie: {}\r\nConnection: close\r\n\r\n",
            port, cookie
        ),
    );
    assert_ne!(r.status, 200);
    // Loopback names on any port are fine (`ssh -L 9000:127.0.0.1:<port>`).
    for ok in [
        format!("localhost:{}", port),
        "localhost:9000".into(),
        format!("[::1]:{}", port),
    ] {
        assert_eq!(
            request(f.server.addr(), "GET", "/", &ok, Some(&cookie)).status,
            200,
            "{}",
            ok
        );
    }
}

#[test]
fn only_get_and_head_are_accepted() {
    let f = fixture(20, Storage::Inline, false);
    let cookie = login(&f);
    for method in ["POST", "PUT", "DELETE", "PATCH", "OPTIONS"] {
        let r = request(f.server.addr(), method, "/", &host(&f), Some(&cookie));
        assert_eq!(r.status, 405, "{}", method);
        assert_eq!(r.header("Allow"), Some("GET, HEAD"));
        assert!(!r.body.contains(&f.op));
        assert_security_headers(&r);
    }
    let r = request(f.server.addr(), "HEAD", "/", &host(&f), Some(&cookie));
    assert_eq!(r.status, 200);
    assert!(r.body.is_empty(), "HEAD has no body: {:?}", r.body);
    assert_security_headers(&r);
}

#[test]
fn every_response_carries_the_security_headers() {
    let f = fixture(20, Storage::Inline, false);
    let cookie = login(&f);
    for (target, status) in [
        ("/".to_string(), 200),
        ("/assets/app.css".to_string(), 200),
        ("/assets/app.js".to_string(), 200),
        (format!("/op/{}", f.op), 200),
        (format!("/api/op/{}/keys", f.op), 200),
        ("/nope".to_string(), 404),
        ("/api/nope".to_string(), 404),
        ("/op/ffff".to_string(), 404),
        // An empty prefix would match every operator.
        ("/op/".to_string(), 404),
        ("/api/op/".to_string(), 404),
        ("/op//keys".to_string(), 404),
        // `+` in a path is a `+`, not a space: no operator id has one.
        (format!("/op/{}+", &f.op[..4]), 404),
        (format!("/op/{}/keys?cursor=garbage", f.op), 400),
        (format!("/op/{}/keys?limit=100000", f.op), 400),
    ] {
        let r = get(&f, &cookie, &target);
        assert_eq!(r.status, status, "{}: {}", target, r.body);
        assert_security_headers(&r);
    }
    let js = get(&f, &cookie, "/assets/app.js");
    assert_eq!(
        js.header("Content-Type"),
        Some("text/javascript; charset=utf-8")
    );
    let css = get(&f, &cookie, "/assets/app.css");
    assert_eq!(css.header("Content-Type"), Some("text/css; charset=utf-8"));
    // `_metadata` is never served as a file, whatever the path.
    for target in [
        "/_metadata",
        "/assets/../_metadata",
        "/assets/%2e%2e/_metadata",
    ] {
        let r = get(&f, &cookie, target);
        assert_eq!(r.status, 404, "{}", target);
    }
}

// --- pages ----------------------------------------------------------------------------------------

#[test]
fn overview_operator_and_key_pages_show_the_state() {
    let f = fixture(40, Storage::Files, false);
    let cookie = login(&f);

    let r = get(&f, &cookie, "/");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("Content-Type"), Some("text/html; charset=utf-8"));
    for state in ["counter", "label", "stats", "history"] {
        assert!(r.body.contains(state), "overview lacks {}", state);
    }
    assert!(r.body.contains("offsets"), "operator state listed");

    let r = get(&f, &cookie, &format!("/op/{}", f.op));
    assert_eq!(r.status, 200);
    for text in [
        "Max parallelism",
        "128",
        "Key type",
        "String",
        "Map&lt;String,Long&gt;",
    ] {
        assert!(r.body.contains(text), "operator page lacks {}", text);
    }
    let source = "51".repeat(16);
    let r = get(&f, &cookie, &format!("/op/{}", source));
    assert!(r.body.contains("offsets") && r.body.contains("SPLIT_DISTRIBUTE"));
    assert!(r.body.contains("use the TUI") || r.body.contains("in the TUI"));

    // The keys page, paged: every key once, across pages, through the "next" links.
    let mut seen = Vec::new();
    let mut target = format!("/op/{}/keys?limit=7", f.op);
    loop {
        let r = get(&f, &cookie, &target);
        assert_eq!(r.status, 200, "{}", target);
        for chunk in r.body.split("key?at=").skip(1) {
            let key = chunk
                .split("\">")
                .nth(1)
                .unwrap()
                .split("</a>")
                .next()
                .unwrap();
            seen.push(key.to_string());
        }
        match r.body.split("<a href=\"").find_map(|c| {
            c.split_once("\">Next page")
                .map(|(href, _)| href.replace("&amp;", "&"))
        }) {
            Some(next) => target = next,
            None => break,
        }
    }
    seen.sort();
    let mut want: Vec<String> = (0..40).map(|i| format!("key-{:07}", i)).collect();
    want.push(web::html::escape(HOSTILE_KEY));
    want.sort();
    assert_eq!(seen, want);

    // One key's page: every state's value, and the raw bytes.
    let api = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key=key-0000003", f.op),
    )
    .json();
    assert_eq!(api["result"], "found", "{}", api);
    let at = api["record"]["position"].as_str().unwrap().to_string();
    let r = get(&f, &cookie, &format!("/op/{}/key?at={}", f.op, at));
    assert_eq!(r.status, 200);
    for text in [
        "key-0000003",
        "<span class=\"v\">100</span>", // counter: 3 * 31 + 7
        "&quot;label of 3&quot;",       // label
        "clicks",                       // map key
        "<span class=\"v\">30</span>",  // history[0]
        "<details class=\"raw\">",
    ] {
        assert!(r.body.contains(text), "key page lacks {}: {}", text, r.body);
    }
    // A position that is not a key's own is not silently another key.
    let r = get(&f, &cookie, &format!("/op/{}/key?at=0.0.999", f.op));
    assert_eq!(r.status, 404);
}

/// The `/api/` rows, paged, are exactly the `--export-state` rows of every state.
#[test]
fn api_rows_equal_export_rows() {
    for (storage, compressed) in [(Storage::Inline, false), (Storage::Files, true)] {
        let f = fixture(60, storage, compressed);
        let cookie = login(&f);
        let sp = parser::parse_metadata(&f.dir.join("_metadata")).unwrap();
        let listing = list_states(&sp, &f.dir);
        assert!(listing.errors.is_empty());
        let mut states = 0;
        for (op, state, _) in listing.states.iter().filter(|(op, _, _)| *op == f.op) {
            states += 1;
            let mut sink = Collect(Vec::new());
            export_state_rows(&sp, &f.dir, Some(op), state, None, &mut sink).unwrap();
            let mut want = sink.0;
            assert!(!want.is_empty(), "{}", state);

            let mut got = Vec::new();
            let mut cursor = String::new();
            loop {
                let r = get(
                    &f,
                    &cookie,
                    &format!(
                        "/api/op/{}/state?name={}&limit=9&cursor={}",
                        f.op,
                        enc(state),
                        cursor
                    ),
                );
                assert_eq!(r.status, 200, "{}", r.body);
                assert_eq!(r.header("Content-Type"), Some("application/json"));
                let v = r.json();
                got.extend(v["rows"].as_array().unwrap().iter().map(|r| r.to_string()));
                match v["next"].as_str() {
                    Some(n) => cursor = n.to_string(),
                    None => break,
                }
            }
            want.sort();
            got.sort();
            assert_eq!(
                got, want,
                "{} ({:?}, compressed {})",
                state, storage, compressed
            );
        }
        assert_eq!(states, 5);

        // The all-states page has the same rows, every state at once.
        let mut all = Vec::new();
        let mut cursor = String::new();
        loop {
            let v = get(
                &f,
                &cookie,
                &format!("/api/op/{}/keys?limit=13&cursor={}", f.op, cursor),
            )
            .json();
            all.extend(v["rows"].as_array().unwrap().iter().map(|r| r.to_string()));
            match v["next"].as_str() {
                Some(n) => cursor = n.to_string(),
                None => break,
            }
        }
        let mut want = Vec::new();
        for (op, state, _) in listing.states.iter().filter(|(op, _, _)| *op == f.op) {
            let mut sink = Collect(Vec::new());
            export_state_rows(&sp, &f.dir, Some(op), state, None, &mut sink).unwrap();
            want.extend(sink.0);
        }
        all.sort();
        want.sort();
        assert_eq!(all, want);
    }
}

struct Collect(Vec<String>);

impl RowSink for Collect {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        self.0.push(row_json(row).to_string());
        Ok(())
    }
    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn lookup_finds_a_key_and_says_where_an_absent_one_would_be() {
    let f = fixture(50, Storage::Files, false);
    let cookie = login(&f);

    let r = get(&f, &cookie, &format!("/op/{}/lookup?key=key-0000042", f.op));
    assert_eq!(r.status, 200);
    assert!(r.body.contains("key-0000042"));
    assert!(r.body.contains("&quot;label of 42&quot;"), "{}", r.body);

    let v = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key=key-0000042", f.op),
    )
    .json();
    assert_eq!(v["result"], "found");
    assert_eq!(
        v["record"]["key_group"].as_i64().unwrap() as i32,
        key_group_for_hash(java_string_hash("key-0000042"), 128)
    );
    let rows = v["record"]["rows"].as_array().unwrap();
    assert!(rows
        .iter()
        .any(|r| r["state"] == "counter" && r["value"] == 42 * 31 + 7));

    let absent = "no-such-key";
    let v = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key={}", f.op, absent),
    )
    .json();
    assert_eq!(v["result"], "absent", "{}", v);
    assert_eq!(
        v["key_group"].as_i64().unwrap() as i32,
        key_group_for_hash(java_string_hash(absent), 128)
    );
    let r = get(&f, &cookie, &format!("/op/{}/lookup?key={}", f.op, absent));
    assert!(r.body.contains("Not found") && r.body.contains("key-group"));

    // The hostile key, looked up by its text.
    let v = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key={}", f.op, enc(HOSTILE_KEY)),
    )
    .json();
    assert_eq!(v["result"], "found", "{}", v);
    assert_eq!(v["record"]["key"], HOSTILE_KEY);
}

#[test]
fn hostile_keys_and_values_are_rendered_inert() {
    let f = fixture(10, Storage::Inline, false);
    let cookie = login(&f);
    let v = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key={}", f.op, enc(HOSTILE_KEY)),
    )
    .json();
    let at = v["record"]["position"].as_str().unwrap().to_string();

    let pages = [
        format!("/op/{}/keys", f.op),
        format!("/op/{}/state?name=label", f.op),
        format!("/op/{}/state?name=stats", f.op),
        format!("/op/{}/key?at={}", f.op, at),
        format!("/op/{}/lookup?key={}", f.op, enc(HOSTILE_KEY)),
        format!("/op/{}/lookup?key={}", f.op, enc(HOSTILE_VALUE)),
    ];
    for page in &pages {
        let r = get(&f, &cookie, page);
        assert_eq!(r.status, 200, "{}", page);
        let body = &r.body;
        for raw in ["<img", "<svg", "onerror=alert(1)>", "</script><script>"] {
            assert!(!body.contains(raw), "{} contains {:?} unescaped", page, raw);
        }
        // The one script tag is the static asset; no data is ever inlined in a script.
        assert_eq!(body.matches("<script").count(), 1, "{}", page);
        assert!(body.contains("<script src=\"/assets/app.js\" defer></script>"));
        assert_security_headers(&r);
    }
    let key_page = get(&f, &cookie, &pages[3]).body;
    assert!(key_page.contains(&web::html::escape(HOSTILE_KEY)));
    assert!(key_page.contains("&lt;/script&gt;&lt;script&gt;alert(1)&lt;/script&gt;"));
    assert!(key_page.contains(&web::html::escape(HOSTILE_MAP_KEY)));
}

#[test]
fn shutdown_stops_the_server() {
    let f = fixture(5, Storage::Inline, false);
    let addr = f.server.addr();
    let cookie = login(&f);
    assert_eq!(get(&f, &cookie, "/").status, 200);
    let Fixture { server, _dir, .. } = f;
    server.shutdown();
    assert!(TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_err());
}

/// What the reader could not read is shown on the page, as a warning naming the subtask, the
/// key-groups and the file, and listed in the JSON twin; the rest of the keys are still served.
#[test]
fn unreadable_key_groups_are_shown_not_hidden() {
    let lost = std::cell::RefCell::new(String::new());
    let f = fixture_with(
        60,
        Storage::Files,
        false,
        |w| {
            let (_, _, path) = w.state_files.iter().find(|(_, s, _)| *s == 1).unwrap();
            *lost.borrow_mut() = path.file_name().unwrap().to_string_lossy().into_owned();
            std::fs::remove_file(path).unwrap();
        },
        None,
    );
    let lost = lost.into_inner();
    let cookie = login(&f);
    let (first, last) = synth::range_for_subtask(128, 2, 1);

    let r = get(&f, &cookie, &format!("/op/{}/keys?limit=1000", f.op));
    assert_eq!(r.status, 200);
    assert!(r.body.contains("class=\"warning\""), "{}", r.body);
    assert!(r.body.contains("could not be read"));
    assert!(r.body.contains(&lost), "the warning names the file");
    assert!(
        r.body
            .contains(&format!("Subtask 1, key-groups {}–{}", first, last)),
        "{}",
        r.body
    );
    assert!(
        r.body.contains("key-0000"),
        "the readable subtask is still listed"
    );

    let v = get(&f, &cookie, &format!("/api/op/{}/keys?limit=1000", f.op)).json();
    let u = v["unreadable"].as_array().unwrap();
    assert_eq!(u.len(), 1, "{}", v);
    assert_eq!(u[0]["subtask"], 1);
    assert_eq!(u[0]["first_key_group"], first);
    assert_eq!(u[0]["last_key_group"], last);
    assert!(u[0]["error"].as_str().unwrap().contains(&lost));
    assert!(!v["rows"].as_array().unwrap().is_empty());

    // A key position in the lost range is an error naming the file, not "no key".
    let r = get(&f, &cookie, &format!("/op/{}/key?at=1.{}.0", f.op, first));
    assert_eq!(r.status, 500, "{}", r.body);
    assert!(r.body.contains(&lost));
    // A lookup of a key the lost subtask owns, too (never "absent").
    let owned = (0..60)
        .map(|i| format!("key-{:07}", i))
        .find(|k| key_group_for_hash(java_string_hash(k), 128) >= first)
        .unwrap();
    let r = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key={}", f.op, owned),
    );
    assert_eq!(r.status, 500, "{}", r.body);
    assert!(r.json()["error"].as_str().unwrap().contains(&lost));
}

/// A key whose state holds more entries than a record carries: its page renders the first ones
/// and links to the paged entries view, which (and its JSON twin) has every entry.
#[test]
fn a_big_key_is_capped_and_its_entries_are_paged() {
    let f = fixture_with(20, Storage::Inline, false, |_| {}, Some(1));
    let cookie = login(&f);
    let v = get(
        &f,
        &cookie,
        &format!("/api/op/{}/lookup?key=key-0000004", f.op),
    )
    .json();
    let rec = &v["record"];
    let at = rec["position"].as_str().unwrap().to_string();
    // `stats` has two map entries (clicks, views): one in the record.
    let stats_rows = rec["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["state"] == "stats")
        .count();
    assert_eq!(stats_rows, 1, "{}", rec);
    let t = rec["truncated"].as_array().unwrap();
    let stats = t
        .iter()
        .find(|t| t["state"] == "stats")
        .expect("stats is truncated");
    assert_eq!(stats["total"], 2);
    assert_eq!(stats["returned"], 1);
    let more = stats["more"].as_str().unwrap().to_string();
    assert!(
        more.starts_with(&format!("/api/op/{}/entries?", f.op)),
        "{}",
        more
    );

    // The key page says so and links to the rest.
    let page = get(&f, &cookie, &format!("/op/{}/key?at={}", f.op, at));
    assert_eq!(page.status, 200);
    assert!(
        page.body.contains("the first 1 of 2 entries"),
        "{}",
        page.body
    );
    let link = page
        .body
        .split("<a href=\"")
        .find_map(|c| {
            c.split_once("\">More entries")
                .map(|(h, _)| h.replace("&amp;", "&"))
        })
        .expect("a More entries link");
    let r = get(&f, &cookie, &link);
    assert_eq!(r.status, 200, "{}: {}", link, r.body);
    assert!(r.body.contains("Entries 2–2 of 2"), "{}", r.body);
    assert!(r.body.contains("views"));
    assert!(r.body.contains("End of this state"));
    assert_eq!(r.body.matches("<section class=\"entry\">").count(), 1);

    // The JSON twin, from 0, pages through every entry, as export rows.
    let v = get(
        &f,
        &cookie,
        &format!(
            "/api/op/{}/entries?at={}&state=stats&offset=0&limit=1",
            f.op, at
        ),
    )
    .json();
    assert_eq!(v["total"], 2);
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);
    assert_eq!(v["rows"][0]["map_key"], "clicks");
    assert_eq!(v["next"], 1);
    let v = get(&f, &cookie, &more).json();
    assert_eq!(v["rows"][0]["map_key"], "views");
    assert_eq!(v["rows"][0]["key"], "key-0000004");
    assert!(v["next"].is_null());

    // The state page has a row linking to the rest.
    let r = get(
        &f,
        &cookie,
        &format!("/op/{}/state?name=stats&limit=1000", f.op),
    );
    assert!(r.body.contains("more entries of this key"), "{}", r.body);
    // Bad parameters.
    for (target, status) in [
        (
            format!("/api/op/{}/entries?at={}&state=nope", f.op, at),
            404,
        ),
        (format!("/api/op/{}/entries?at={}", f.op, at), 400),
        (format!("/api/op/{}/entries?state=stats", f.op), 400),
        (
            format!("/api/op/{}/entries?at={}&state=stats&offset=-1", f.op, at),
            400,
        ),
        (
            format!("/api/op/{}/entries?at=0.0.999&state=stats", f.op),
            404,
        ),
    ] {
        let r = get(&f, &cookie, &target);
        assert_eq!(r.status, status, "{}: {}", target, r.body);
    }
}

/// Past [`web::MAX_CONNECTIONS`] open connections, one more is answered 503 (with the security
/// headers) without being served; once they close, the server answers again.
#[test]
fn connections_beyond_the_limit_get_503() {
    let f = fixture(5, Storage::Inline, false);
    let cookie = login(&f);
    let idle: Vec<TcpStream> = (0..web::MAX_CONNECTIONS)
        .map(|_| TcpStream::connect(f.server.addr()).unwrap())
        .collect();
    let r = get(&f, &cookie, "/");
    assert_eq!(r.status, 503, "{:?}", r);
    assert!(!r.body.contains(&f.op));
    assert_eq!(r.header("Retry-After"), Some("1"));
    assert_security_headers(&r);
    drop(idle);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = get(&f, &cookie, "/");
        if r.status == 200 {
            break;
        }
        assert_eq!(r.status, 503);
        assert!(
            std::time::Instant::now() < deadline,
            "the permits came back"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Many parallel requests are all answered (the read permits queue them, none is refused).
#[test]
fn parallel_requests_beyond_the_read_permits_all_succeed() {
    let f = fixture(200, Storage::Files, false);
    let cookie = login(&f);
    let n = (web::max_concurrent_reads() * 3).min(web::MAX_CONNECTIONS - 1);
    let addr = f.server.addr();
    let host = host(&f);
    let target = format!("/api/op/{}/keys?limit=1000", f.op);
    let threads: Vec<_> = (0..n)
        .map(|_| {
            let (host, cookie, target) = (host.clone(), cookie.clone(), target.clone());
            std::thread::spawn(move || request(addr, "GET", &target, &host, Some(&cookie)).status)
        })
        .collect();
    for t in threads {
        assert_eq!(t.join().unwrap(), 200);
    }
}
