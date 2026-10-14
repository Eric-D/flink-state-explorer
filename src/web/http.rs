//! The HTTP/1.1 server: hyper 1 on a hand-built single-threaded tokio runtime.
//!
//! This is the only file that knows about hyper or tokio; everything it does with a request is
//! [`App::handle`], run on tokio's blocking pool since it reads the savepoint. Swapping the
//! server means rewriting this file alone.

use std::convert::Infallible;
use std::future::{poll_fn, Future};
use std::net::{SocketAddr, TcpListener as StdListener};
use std::pin::{pin, Pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::header::{HeaderName, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};

use super::{
    busy, new_token, too_many_downloads, with_security_headers, App, Request, Response, StreamFn,
    WebOptions,
};
use crate::reader::StateReader;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type ServerBody = UnsyncBoxBody<Bytes, BoxError>;

/// A streamed body is sent in chunks of about this size.
const CHUNK_BYTES: usize = 64 << 10;
/// Chunks in flight between the producer and the connection: the producer blocks beyond this,
/// so a slow client holds `CHUNKS_IN_FLIGHT × CHUNK_BYTES`, not the whole download.
const CHUNKS_IN_FLIGHT: usize = 4;

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

/// The limits, shared by every connection.
struct Limits {
    connections: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    /// Downloads streaming at once ([`WebOptions::max_downloads`]): a download holds one of these
    /// for its whole life, and no read permit, so a client that stops reading blocks nothing
    /// but its own slot (until [`WebOptions::download_stall`]).
    downloads: Arc<Semaphore>,
}

/// Whether the connection's streamed body waits on its client: armed when a chunk is handed to
/// hyper, disarmed when hyper asks for the next one (it does so once it could write the previous
/// ones) or the body ends. Armed for longer than [`WebOptions::download_stall`] = the client
/// takes nothing: the connection is closed, which ends the download.
#[derive(Default)]
struct StallWatch {
    since: Mutex<Option<Instant>>,
}

impl StallWatch {
    fn set(&self, armed: bool) {
        let mut since = match self.since.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        *since = armed.then(Instant::now);
    }

    fn stalled(&self, limit: Duration) -> bool {
        let since = match self.since.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        since.is_some_and(|t| t.elapsed() >= limit)
    }
}

/// Drive `conn` to its end, unless its download stalls ([`StallWatch`]): then drop it, which
/// closes the socket, drops the body's receiver and so stops the producer.
async fn serve_watched<F: Future>(conn: F, watch: &StallWatch, limit: Duration) {
    let mut conn = pin!(conn);
    let mut tick =
        tokio::time::interval((limit / 4).clamp(Duration::from_millis(50), Duration::from_secs(5)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    poll_fn(|cx| {
        if conn.as_mut().poll(cx).is_ready() {
            return Poll::Ready(());
        }
        while tick.poll_tick(cx).is_ready() {
            if watch.stalled(limit) {
                eprintln!(
                    "web server: a download's client took nothing for {} s: aborted",
                    limit.as_secs_f64()
                );
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await
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
    start_with(reader, bind, false, WebOptions::default())
}

/// [`start`] with [`WebOptions`].
pub fn start_with_options(
    reader: StateReader<'static>,
    bind: SocketAddr,
    options: WebOptions,
) -> Result<RunningServer, String> {
    start_with(reader, bind, false, options)
}

/// Like [`start`], and also stop on Ctrl-C (for the command line: [`RunningServer::wait`]).
pub fn serve(reader: StateReader<'static>, bind: SocketAddr) -> Result<RunningServer, String> {
    start_with(reader, bind, true, WebOptions::default())
}

fn start_with(
    reader: StateReader<'static>,
    bind: SocketAddr,
    ctrl_c: bool,
    options: WebOptions,
) -> Result<RunningServer, String> {
    let listener = StdListener::bind(bind).map_err(|e| format!("bind {}: {}", bind, e))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("bind {}: {}", bind, e))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("bind {}: {}", bind, e))?;
    let app = Arc::new(App::with_options(
        Arc::new(reader),
        new_token()?,
        addr,
        options,
    ));
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
        downloads: Arc::new(Semaphore::new(app.options.max_downloads.max(1))),
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
                    let downloads = limits.downloads.clone();
                    let watch = Arc::new(StallWatch::default());
                    tokio::spawn(async move {
                        let stall = app.options.download_stall;
                        let w = watch.clone();
                        let service = service_fn(move |req| {
                            respond(
                                app.clone(),
                                reads.clone(),
                                downloads.clone(),
                                w.clone(),
                                req,
                            )
                        });
                        let conn = http1::Builder::new()
                            .timer(TokioTimer::new())
                            .header_read_timeout(HEADER_TIMEOUT)
                            .serve_connection(TokioIo::new(stream), service);
                        serve_watched(conn, &watch, stall).await;
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
    downloads: Arc<Semaphore>,
    watch: Arc<StallWatch>,
    req: hyper::Request<Incoming>,
) -> Result<hyper::Response<ServerBody>, Infallible> {
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
    // went away still counts until its read is over. A download's body is not read under it.
    let Ok(permit) = reads.acquire_owned().await else {
        return Ok(to_hyper(busy()));
    };
    let handled = tokio::task::spawn_blocking(move || {
        let mut resp = app.handle(&request);
        drop(permit);
        let producer = resp.stream.take().and_then(|s| s.take());
        (resp, producer)
    })
    .await;
    let (resp, producer) = match handled {
        Ok(r) => r,
        Err(_) => return Ok(to_hyper(internal_error())),
    };
    match producer {
        None => Ok(to_hyper(resp)),
        // A download slot, or a `503` at once: never a wait behind stalled downloads.
        Some(produce) => match downloads.try_acquire_owned() {
            Ok(slot) => Ok(streamed(resp, produce, slot, watch).await),
            Err(_) => Ok(to_hyper(too_many_downloads())),
        },
    }
}

fn internal_error() -> Response {
    let mut r = Response::text(500, "Internal Server Error\n");
    with_security_headers(&mut r);
    r
}

/// What the producer of a streamed body sends to the connection.
enum Chunk {
    Data(Bytes),
    /// The producer failed: abort the body (or answer `500` if nothing was sent).
    Fail(String),
    /// The body is complete.
    Done,
}

/// Run `produce` on a blocking thread (holding the download `slot`), and answer `resp`'s headers
/// with the chunks it writes. Its first message decides: data → `200` and a chunked body; a
/// failure before any data → a `500` (its cause logged, not sent), never a truncated `200`.
async fn streamed(
    resp: Response,
    produce: StreamFn,
    slot: OwnedSemaphorePermit,
    watch: Arc<StallWatch>,
) -> hyper::Response<ServerBody> {
    let (tx, mut rx) = mpsc::channel::<Chunk>(CHUNKS_IN_FLIGHT);
    tokio::task::spawn_blocking(move || {
        let mut w = ChannelWriter {
            tx: tx.clone(),
            buf: Vec::with_capacity(CHUNK_BYTES),
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| produce(&mut w)))
            .unwrap_or_else(|_| Err("the download failed (internal error)".to_string()))
            .and_then(|()| w.send_buffered().map_err(|e| e.to_string()));
        // The client may be gone: nothing to tell then.
        let _ = tx.blocking_send(match result {
            Ok(()) => Chunk::Done,
            Err(e) => Chunk::Fail(e),
        });
        drop(slot);
    });
    match rx.recv().await {
        Some(Chunk::Data(first)) => {
            let body = ChannelBody {
                first: Some(first),
                rx,
                done: false,
                watch,
            };
            with_body(resp, body.boxed_unsync())
        }
        Some(Chunk::Done) => to_hyper(resp),
        // The cause names files of the savepoint: it goes to the log, not to the client.
        Some(Chunk::Fail(e)) => {
            eprintln!("web server: download failed: {}", e);
            let mut r = Response::text(
                500,
                "Internal Server Error: this state could not be read; the cause is in the \
                 server's log.\n",
            );
            with_security_headers(&mut r);
            to_hyper(r)
        }
        None => to_hyper(internal_error()),
    }
}

/// A `Write` that sends what it is given to the connection in [`CHUNK_BYTES`] chunks, blocking
/// while [`CHUNKS_IN_FLIGHT`] are queued; it fails once the connection is gone, which stops the
/// producer.
struct ChannelWriter {
    tx: mpsc::Sender<Chunk>,
    buf: Vec<u8>,
}

impl ChannelWriter {
    fn send_buffered(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK_BYTES));
        self.tx
            .blocking_send(Chunk::Data(Bytes::from(chunk)))
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the client went away")
            })
    }
}

impl std::io::Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= CHUNK_BYTES {
            self.send_buffered()?;
        }
        Ok(data.len())
    }

    /// Chunks go out when full or at the end: a sink's flush does not make small chunks.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The body of a streamed response: the chunks of a [`ChannelWriter`]. It ends cleanly only on
/// [`Chunk::Done`]; a failure, or a producer gone without saying so, is an error, which makes
/// hyper abort the connection without the final chunk — a client sees a failed transfer, not a
/// short file.
struct ChannelBody {
    first: Option<Bytes>,
    rx: mpsc::Receiver<Chunk>,
    done: bool,
    watch: Arc<StallWatch>,
}

impl Drop for ChannelBody {
    fn drop(&mut self) {
        self.watch.set(false);
    }
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        // Asked for more: what was handed over is written (or buffered). Waiting on the producer
        // is not the client's stall.
        self.watch.set(false);
        if let Some(first) = self.first.take() {
            self.watch.set(true);
            return Poll::Ready(Some(Ok(Frame::data(first))));
        }
        if self.done {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Chunk::Data(b))) => {
                self.watch.set(true);
                Poll::Ready(Some(Ok(Frame::data(b))))
            }
            Poll::Ready(Some(Chunk::Done)) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Chunk::Fail(e))) => {
                self.done = true;
                eprintln!("web server: download aborted: {}", e);
                Poll::Ready(Some(Err(e.into())))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(Some(Err("the download ended early".into())))
            }
        }
    }
}

fn to_hyper(mut resp: Response) -> hyper::Response<ServerBody> {
    let body = Full::new(Bytes::from(std::mem::take(&mut resp.body)))
        .map_err(|never: Infallible| match never {})
        .boxed_unsync();
    with_body(resp, body)
}

fn with_body(resp: Response, body: ServerBody) -> hyper::Response<ServerBody> {
    let mut out = hyper::Response::new(body);
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
