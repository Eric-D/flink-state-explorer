//! The HTTP/1.1 server: hyper 1 on a hand-built single-threaded tokio runtime.
//!
//! This is the only file that knows about hyper or tokio; everything it does with a request is
//! [`App::handle`], run on tokio's blocking pool since it reads the savepoint. Swapping the
//! server means rewriting this file alone.

use std::convert::Infallible;
use std::future::{poll_fn, Future};
use std::net::{SocketAddr, TcpListener as StdListener};
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::thread::JoinHandle;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderName, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::sync::{Notify, Semaphore};

use super::{busy, new_token, with_security_headers, App, Request, Response};
use crate::reader::StateReader;

/// A request's headers must arrive within this time (a client that trickles them holds a
/// connection, nothing more, but not forever).
const HEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// Connections served at once. One more is answered `503` and closed, without reading the
/// savepoint: a local page (or a script) opening hundreds cannot make the server hold hundreds.
pub const MAX_CONNECTIONS: usize = 64;

/// Requests reading the savepoint at once: about two per CPU. Each holds a page of keys and its
/// rendering in memory (400 large pages in parallel took the process to 1.1 GB); the others wait
/// for a permit, so memory is bounded by this many pages.
pub fn max_concurrent_reads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(4)
        .clamp(2, 64)
}

/// The two limits, shared by every connection.
struct Limits {
    connections: Arc<Semaphore>,
    reads: Arc<Semaphore>,
}

/// A server running on its own thread. Dropping it stops it.
pub struct RunningServer {
    addr: SocketAddr,
    url: String,
    cookie_name: String,
    stop: Arc<Notify>,
    thread: Option<JoinHandle<()>>,
}

impl RunningServer {
    /// The address actually bound (the port is the real one when 0 was asked for).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The first URL to open: it carries the session token.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The name of the session cookie the token is exchanged for.
    pub fn cookie_name(&self) -> &str {
        &self.cookie_name
    }

    /// Stop accepting, drop open connections, and wait for the server thread.
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    /// Block until the server stops (on Ctrl-C when started by [`serve`]).
    pub fn wait(mut self) {
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }

    fn stop_and_join(&mut self) {
        self.stop.notify_one();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// Bind `bind` (port 0 = any free port) and serve `reader` on a background thread.
pub fn start(reader: StateReader<'static>, bind: SocketAddr) -> Result<RunningServer, String> {
    start_with(reader, bind, false)
}

/// Like [`start`], and also stop on Ctrl-C (for the command line: [`RunningServer::wait`]).
pub fn serve(reader: StateReader<'static>, bind: SocketAddr) -> Result<RunningServer, String> {
    start_with(reader, bind, true)
}

fn start_with(
    reader: StateReader<'static>,
    bind: SocketAddr,
    ctrl_c: bool,
) -> Result<RunningServer, String> {
    let listener = StdListener::bind(bind).map_err(|e| format!("bind {}: {}", bind, e))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("bind {}: {}", bind, e))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("bind {}: {}", bind, e))?;
    let app = Arc::new(App::new(Arc::new(reader), new_token()?, addr));
    let url = app.login_url();
    let cookie_name = app.cookie_name();
    let stop = Arc::new(Notify::new());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| format!("start the web server: {}", e))?;
    let thread_stop = stop.clone();
    let thread = std::thread::Builder::new()
        .name("flink-explorer-web".into())
        .spawn(move || {
            runtime.block_on(async move {
                match tokio::net::TcpListener::from_std(listener) {
                    Ok(l) => accept_loop(l, app, thread_stop, ctrl_c).await,
                    Err(e) => eprintln!("web server: {}", e),
                }
            });
            // Requests still reading a block get a moment to finish, then are dropped.
            runtime.shutdown_timeout(Duration::from_secs(2));
        })
        .map_err(|e| format!("start the web server: {}", e))?;
    Ok(RunningServer {
        addr,
        url,
        cookie_name,
        stop,
        thread: Some(thread),
    })
}

async fn accept_loop(
    listener: tokio::net::TcpListener,
    app: Arc<App>,
    stop: Arc<Notify>,
    ctrl_c: bool,
) {
    if ctrl_c {
        let stop = stop.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                stop.notify_one();
            }
        });
    }
    let limits = Limits {
        connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        reads: Arc::new(Semaphore::new(max_concurrent_reads())),
    };
    let mut stopped = pin!(stop.notified());
    loop {
        let accepted = poll_fn(|cx| {
            if stopped.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            listener.poll_accept(cx).map(Some)
        })
        .await;
        match accepted {
            None => return,
            Some(Ok((stream, _peer))) => match limits.connections.clone().try_acquire_owned() {
                Ok(permit) => {
                    let app = app.clone();
                    let reads = limits.reads.clone();
                    tokio::spawn(async move {
                        let service =
                            service_fn(move |req| respond(app.clone(), reads.clone(), req));
                        let _ = http1::Builder::new()
                            .timer(TokioTimer::new())
                            .header_read_timeout(HEADER_TIMEOUT)
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                        drop(permit);
                    });
                }
                // Over the limit: one `503`, then the connection is closed. Nothing is read
                // from the savepoint, and the header timeout still bounds a silent client.
                Err(_) => {
                    tokio::spawn(async move {
                        let service =
                            service_fn(|_req| async { Ok::<_, Infallible>(to_hyper(busy())) });
                        let _ = http1::Builder::new()
                            .timer(TokioTimer::new())
                            .header_read_timeout(HEADER_TIMEOUT)
                            .keep_alive(false)
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
            },
            // E.g. out of file descriptors: do not spin.
            Some(Err(e)) => {
                eprintln!("web server: accept: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn respond(
    app: Arc<App>,
    reads: Arc<Semaphore>,
    req: hyper::Request<Incoming>,
) -> Result<hyper::Response<Full<Bytes>>, Infallible> {
    let header = |name: &str| {
        req.headers()
            .get_all(name)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect::<Vec<_>>()
    };
    let hosts = header("host");
    // Exactly one Host header; an absolute-form request target must name the same host.
    let host = match (hosts.as_slice(), req.uri().authority()) {
        ([h], None) => Some(h.clone()),
        ([h], Some(a)) if a.as_str().eq_ignore_ascii_case(h) => Some(h.clone()),
        _ => None,
    };
    let cookies = header("cookie");
    let request = Request {
        method: req.method().as_str().to_string(),
        path: req.uri().path().to_string(),
        query: req.uri().query().unwrap_or("").to_string(),
        host,
        cookie: (!cookies.is_empty()).then(|| cookies.join("; ")),
    };
    // Wait for a read permit; it is held by the blocking task itself, so a request whose client
    // went away still counts until its read is over.
    let Ok(permit) = reads.acquire_owned().await else {
        return Ok(to_hyper(busy()));
    };
    let resp = tokio::task::spawn_blocking(move || {
        let resp = app.handle(&request);
        drop(permit);
        resp
    })
    .await
    .unwrap_or_else(|_| {
        let mut r = Response::text(500, "Internal Server Error\n");
        with_security_headers(&mut r);
        r
    });
    Ok(to_hyper(resp))
}

fn to_hyper(resp: Response) -> hyper::Response<Full<Bytes>> {
    let mut out = hyper::Response::new(Full::new(Bytes::from(resp.body)));
    *out.status_mut() = hyper::StatusCode::from_u16(resp.status)
        .unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR);
    let headers = out.headers_mut();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        HeaderValue::from_static(resp.content_type),
    );
    for (name, value) in resp.headers {
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.append(n, v);
        }
    }
    out
}
