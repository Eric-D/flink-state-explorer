//! `flink-explorer <savepoint> --web`: a read-only browser UI over [`crate::reader::StateReader`].
//!
//! Server-rendered HTML (plus a little progressive JavaScript, embedded) and JSON twins under
//! `/api/`. Every page is a URL: overview, operator, key pages (cursor paging), key detail, lookup,
//! search, counts and downloads (`tools.rs`, `jobs.rs`).
//!
//! The server publishes production data, so the request path is guarded in this order, by
//! [`App::handle`], for every request including static assets:
//!
//! 1. only `GET`/`HEAD` (405 otherwise);
//! 2. the `Host` header must name the server (a loopback name, or the address it is bound to):
//!    a DNS-rebinding page has its own host name there;
//! 3. a random 128-bit token, printed once in the startup URL, is exchanged for an `HttpOnly`,
//!    `SameSite=Strict` cookie, set by a `200` page that then moves to the same URL without the
//!    token through `<meta http-equiv="refresh">` (not a `303`: a token link followed from
//!    another site makes a cross-site navigation, whose redirect a browser may serve without
//!    the strict cookie it has just set; the refresh is a navigation started by this origin);
//!    without the cookie, 403 and no data;
//! 4. every response carries a strict CSP, `nosniff`, `no-referrer`, `no-store`, and
//!    same-origin `Cross-Origin-Resource-Policy` / `Cross-Origin-Opener-Policy`.
//!
//! HTML escaping is a type invariant ([`html::Html`]). Values are rendered from the
//! `serde_json::Value` that [`crate::parser::value`] produces: this module never decodes bytes.
//! Paths read from `_metadata` are never served as files: the only files served are the two
//! embedded assets.
//!
//! The HTTP plumbing (hyper/tokio) is confined to [`http`]; everything else is plain functions
//! from a [`Request`] to a [`Response`].

pub mod html;
pub mod http;
mod jobs;
mod pages;
mod tools;

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::reader::StateReader;

pub use http::{
    max_concurrent_reads, serve, start, start_with_options, RunningServer, MAX_CONNECTIONS,
};
pub use pages::ENTRY_LIMIT;
pub use tools::{download_filename, DEFAULT_SEARCH_BUDGET, MAX_QUERY_CHARS, MAX_SEARCH_BUDGET};

/// Tunables of the server that are not per request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebOptions {
    /// Stored bytes a count job reads per step; it checks for cancellation between steps.
    pub count_step_bytes: u64,
    /// A pause between two count steps: zero in use; tests set one to catch a job running.
    pub count_step_pause: Duration,
}

impl Default for WebOptions {
    fn default() -> Self {
        WebOptions {
            count_step_bytes: 4 << 20,
            count_step_pause: Duration::ZERO,
        }
    }
}

/// What the server needs to know about one request.
#[derive(Debug, Clone, Default)]
pub struct Request {
    pub method: String,
    /// The path, still percent-encoded.
    pub path: String,
    /// The raw query string, without `?` (empty if none).
    pub query: String,
    pub host: Option<String>,
    /// Every `Cookie` header, joined with `; `.
    pub cookie: Option<String>,
}

/// A complete response.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub content_type: &'static str,
    /// Extra headers (the security headers are added by [`App::handle`]).
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
    /// A body produced while it is sent (a download): run by [`http`] on a blocking thread that
    /// holds a read permit, written through a bounded channel as a chunked body. `body` is then
    /// empty. If the producer fails before its first chunk, the client gets a `500` instead.
    pub stream: Option<Streamed>,
}

/// What produces a streamed body: it writes the whole body to the writer, or fails.
pub type StreamFn = Box<dyn FnOnce(&mut (dyn std::io::Write + Send)) -> Result<(), String> + Send>;

/// A [`StreamFn`], taken once.
#[derive(Clone)]
pub struct Streamed(Arc<Mutex<Option<StreamFn>>>);

impl Streamed {
    pub fn new(f: StreamFn) -> Self {
        Streamed(Arc::new(Mutex::new(Some(f))))
    }

    /// The producer, the first time only.
    pub fn take(&self) -> Option<StreamFn> {
        self.0.lock().ok().and_then(|mut f| f.take())
    }
}

impl std::fmt::Debug for Streamed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Streamed(..)")
    }
}

impl Response {
    fn new(status: u16, content_type: &'static str, body: impl Into<Vec<u8>>) -> Self {
        Response {
            status,
            content_type,
            headers: Vec::new(),
            body: body.into(),
            stream: None,
        }
    }

    fn text(status: u16, body: &str) -> Self {
        Self::new(
            status,
            "text/plain; charset=utf-8",
            body.as_bytes().to_vec(),
        )
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// The headers every response carries.
pub const SECURITY_HEADERS: [(&str, &str); 7] = [
    (
        "Content-Security-Policy",
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; \
         connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
    ),
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "no-referrer"),
    ("Cache-Control", "no-store"),
    ("X-Frame-Options", "DENY"),
    // No other origin may embed a response (`<img>`, `<script>`, …) nor keep a handle on a
    // window of this one.
    ("Cross-Origin-Resource-Policy", "same-origin"),
    ("Cross-Origin-Opener-Policy", "same-origin"),
];

/// A fresh 128-bit token from the operating system's CSPRNG, as 32 hex digits.
pub fn new_token() -> Result<String, String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| format!("no OS randomness for the token: {}", e))?;
    Ok(bytes.iter().map(|b| format!("{:02x}", b)).collect())
}

/// The web application: the reader, the token and the address it answers for.
pub struct App {
    pub(crate) reader: Arc<StateReader<'static>>,
    token: String,
    bound: SocketAddr,
    /// Shown in the page header.
    pub(crate) title: String,
    pub(crate) options: WebOptions,
    /// The count jobs of this session, one per operator at most.
    pub(crate) counts: jobs::CountJobs,
}

impl App {
    pub fn new(reader: Arc<StateReader<'static>>, token: String, bound: SocketAddr) -> Self {
        Self::with_options(reader, token, bound, WebOptions::default())
    }

    pub fn with_options(
        reader: Arc<StateReader<'static>>,
        token: String,
        bound: SocketAddr,
        options: WebOptions,
    ) -> Self {
        let title = reader
            .savepoint_dir()
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| reader.savepoint_dir().display().to_string());
        App {
            reader,
            token,
            bound,
            title,
            options,
            counts: jobs::CountJobs::default(),
        }
    }

    /// The URL to open first: it carries the token.
    pub fn login_url(&self) -> String {
        format!("http://{}/?token={}", url_host(self.bound), self.token)
    }

    /// The session cookie's name. Cookies are not scoped by port, so two servers on one host
    /// would overwrite each other's cookie under a fixed name.
    pub fn cookie_name(&self) -> String {
        format!("flink_explorer_{}", self.bound.port())
    }

    /// Answer one request: the guards of the [module documentation](self), then the routes.
    /// Reads the savepoint, so call it off the async runtime.
    pub fn handle(&self, req: &Request) -> Response {
        let mut resp = self.guarded(req);
        with_security_headers(&mut resp);
        if req.method == "HEAD" {
            // A streamed body is not produced for a HEAD, and its length is unknown.
            if resp.stream.take().is_none() {
                resp.headers
                    .push(("Content-Length", resp.body.len().to_string()));
            }
            resp.body.clear();
        }
        resp
    }

    fn guarded(&self, req: &Request) -> Response {
        if req.method != "GET" && req.method != "HEAD" {
            let mut r = Response::text(405, "Method Not Allowed: this server is read-only.\n");
            r.headers.push(("Allow", "GET, HEAD".to_string()));
            return r;
        }
        if !host_allowed(req.host.as_deref(), self.bound) {
            return Response::text(
                403,
                "Forbidden: unexpected Host header. Open the URL printed in the terminal.\n",
            );
        }
        let query = parse_query(&req.query);
        if let Some((_, token)) = query.iter().find(|(k, _)| k == "token") {
            if !constant_time_eq(token.as_bytes(), self.token.as_bytes()) {
                return Response::text(403, "Forbidden: wrong token.\n");
            }
            // Same URL, minus the token: it should not stay in the address bar or history.
            let rest: Vec<String> = query
                .iter()
                .filter(|(k, _)| k != "token")
                .map(|(k, v)| format!("{}={}", html::url_encode(k), html::url_encode(v)))
                .collect();
            // Never an open redirect: `//host` and `/\host` are other origins to a browser.
            let path = if req.path.starts_with('/')
                && !req.path.starts_with("//")
                && !req.path.starts_with("/\\")
            {
                req.path.as_str()
            } else {
                "/"
            };
            let location = if rest.is_empty() {
                path.to_string()
            } else {
                format!("{}?{}", path, rest.join("&"))
            };
            let mut r = Response::new(
                200,
                "text/html; charset=utf-8",
                login_page(&location).into_string().into_bytes(),
            );
            r.headers.push((
                "Set-Cookie",
                format!(
                    "{}={}; HttpOnly; SameSite=Strict; Path=/",
                    self.cookie_name(),
                    self.token
                ),
            ));
            return r;
        }
        if !self.has_session(req.cookie.as_deref()) {
            return Response::text(
                403,
                "Forbidden: no session. Open the URL with the token, as printed in the terminal \
                 where flink-explorer runs.\n",
            );
        }
        pages::route(self, &req.path, &query)
    }

    fn has_session(&self, cookie: Option<&str>) -> bool {
        let name = self.cookie_name();
        cookie.is_some_and(|c| {
            c.split(';').any(|pair| {
                let (k, v) = pair.trim().split_once('=').unwrap_or((pair.trim(), ""));
                k == name && constant_time_eq(v.as_bytes(), self.token.as_bytes())
            })
        })
    }
}

/// What a connection gets when the server already has as many as it accepts: a `503`, with the
/// security headers, that reads nothing.
pub fn busy() -> Response {
    let mut r = Response::text(
        503,
        "Service Unavailable: too many connections; retry in a moment.\n",
    );
    r.headers.push(("Retry-After", "1".to_string()));
    with_security_headers(&mut r);
    r
}

pub(crate) fn with_security_headers(resp: &mut Response) {
    for (name, value) in SECURITY_HEADERS {
        resp.headers.push((name, value.to_string()));
    }
}

/// The page answering the token: it has just set the cookie, and moves on to `location` (a
/// same-origin path) by itself, or through its link. `meta refresh` is not governed by the CSP.
fn login_page(location: &str) -> html::Html {
    let mut h = html::Html::new();
    h.raw("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">")
        .raw("<meta name=\"referrer\" content=\"no-referrer\">")
        .raw("<meta http-equiv=\"refresh\" content=\"0;url=")
        .text(location)
        .raw("\"><title>Signed in · flink-explorer</title></head><body><p>Signed in. <a href=\"")
        .text(location)
        .raw("\">Continue</a></p></body></html>\n");
    h
}

/// `ip:port` as a URL authority (`[v6]:port`); `localhost`'s address for an unspecified bind.
fn url_host(addr: SocketAddr) -> String {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => format!("127.0.0.1:{}", addr.port()),
        IpAddr::V6(ip) if ip.is_unspecified() => format!("[::1]:{}", addr.port()),
        IpAddr::V4(ip) => format!("{}:{}", ip, addr.port()),
        IpAddr::V6(ip) => format!("[{}]:{}", ip, addr.port()),
    }
}

/// Whether the `Host` header names this server.
///
/// DNS rebinding makes a browser send a request to us under an attacker's **host name**; the
/// browser then puts that name in `Host`. So the name is what is checked: a loopback name
/// (`localhost`, `127.0.0.1`, `[::1]`) always, plus the bound address itself when bound elsewhere
/// (any IP literal when bound to the unspecified address — an IP literal cannot be rebound). The
/// port is not checked, so `ssh -L 9000:127.0.0.1:8080` works as well as `ssh -L 8080:…:8080`.
pub fn host_allowed(host: Option<&str>, bound: SocketAddr) -> bool {
    let Some(host) = host else {
        return false;
    };
    let host = host.trim().to_ascii_lowercase();
    // Strip the port: `[v6]:port`, `name:port`, or none.
    let name = if let Some(rest) = host.strip_prefix('[') {
        match rest.split_once(']') {
            Some((inside, after)) if after.is_empty() || after.starts_with(':') => inside,
            _ => return false,
        }
    } else {
        match host.rsplit_once(':') {
            Some((name, port)) if port.chars().all(|c| c.is_ascii_digit()) => name,
            Some(_) => return false,
            None => host.as_str(),
        }
    };
    if name == "localhost" {
        return true;
    }
    let Ok(ip) = name.parse::<IpAddr>() else {
        return false;
    };
    ip.is_loopback() || ip == bound.ip() || bound.ip().is_unspecified()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `a=1&b=x%20y` → decoded pairs, in order.
pub(crate) fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (html::url_decode(k), html::url_decode(v))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn hosts() {
        let lo = addr("127.0.0.1:8080");
        for ok in [
            "127.0.0.1:8080",
            "localhost:8080",
            "LOCALHOST",
            "[::1]:9000",
            "127.0.0.1:9000",
        ] {
            assert!(host_allowed(Some(ok), lo), "{ok}");
        }
        for bad in [
            "evil.example:8080",
            "evil.example",
            "192.168.1.5:8080",
            "localhost.evil.example:8080",
            "127.0.0.1:80x",
            "[::1]x",
            "",
        ] {
            assert!(!host_allowed(Some(bad), lo), "{bad}");
        }
        assert!(!host_allowed(None, lo));
        let lan = addr("192.168.1.5:8080");
        assert!(host_allowed(Some("192.168.1.5:8080"), lan));
        assert!(!host_allowed(Some("192.168.1.6:8080"), lan));
        assert!(!host_allowed(Some("evil.example"), lan));
        let any = addr("0.0.0.0:8080");
        assert!(host_allowed(Some("10.0.0.3:8080"), any));
        assert!(!host_allowed(Some("evil.example"), any));
    }

    #[test]
    fn tokens_are_random_and_128_bits() {
        let a = new_token().unwrap();
        let b = new_token().unwrap();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }
}
