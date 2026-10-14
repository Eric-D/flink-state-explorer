# CraftFlinkSavepointExplorer Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-03-31

## Active Technologies

- Rust (stable, edition 2021) (001-flink-savepoint-explorer)

## Project Structure

```text
src/
tests/
```

## Commands

cargo test; cargo clippy

## Code Style

Rust (stable, edition 2021): Follow standard conventions

## Recent Changes

- 001-flink-savepoint-explorer: Added Rust (stable, edition 2021)

<!-- MANUAL ADDITIONS START -->

## What this is

A Rust TUI (`ratatui`) that parses **Apache Flink 1.20 canonical savepoints** (heap/HashMap
backend) and lets you browse operators → keys → states → values. `test-flink-job/` is a Java
Flink job that generates a savepoint exercising every state type, used as the test fixture.

## Generating / regenerating the test savepoint (Testcontainers)

`test-flink-job/src/test/java/.../SavepointGeneratorIT.java` spins up a real Flink 1.20 cluster
in Docker (Testcontainers), runs `ClusterJob`, triggers a CANONICAL savepoint, and writes it to
`tests/fixtures/savepoint-gitlab/` (gitignored — regenerated, not committed).

```bash
cd test-flink-job && mvn verify          # builds shaded jar, then generates the savepoints (~3 min)
cd .. && cargo test                      # Rust tests read the fixtures below
```

Three ITs generate three fixtures (all gitignored), plus an opt-in fourth: `SavepointGeneratorIT` → `savepoint-gitlab`
(all state types, via `ClusterJob`) and → `savepoint-compressed` (the same job run with
`compress`, i.e. `ExecutionConfig.setUseSnapshotCompression`, so each key-group is an independent
Snappy-framed stream — the only fixture covering the compressed read path; both tests share one
cluster and cancel their job afterwards so the slots are free); `KafkaSavepointGeneratorIT` →
`savepoint-kafka`
(KafkaSource→KafkaSink via `KafkaJob` + a Testcontainers Kafka broker). The Kafka IT is separate so
a broker hiccup can't break the main fixture; records are produced from inside the broker and the
Flink containers reach it via the in-network listener `kafka:19092`. Gotchas handled: the Kafka
sink's default `transaction.timeout.ms` (1h) exceeds the broker's `transaction.max.timeout.ms`
(15min) → set to 600000 in `KafkaJob`; the connector pulls an older `jackson-core` onto the test
classpath → pinned to 2.17.2 in the pom.

`BigSavepointGeneratorIT` (skipped unless `-Dbig.savepoint=true`) → `savepoint-big`: `BigStateJob`,
1M String keys × (ValueState<Long>, ValueState<String>, MapState<String,Long>), ~211 MB, written
with **filesystem** checkpoint storage so each subtask's keyed state is a separate file next to
`_metadata` (`RelativeFileStateHandle`) — the layout of production savepoints. Every value is a
pure function of the key index (`BigStateJob.*Of`). For performance work:
`mvn -B -Dbig.savepoint=true -Dit.test=BigSavepointGeneratorIT verify`, then
`cargo run --release --example bench_tui -- tests/fixtures/savepoint-big [keys to page] [glob]`
(the TUI path, headless on a `TestBackend`: open + first frame, expand + first page, paging N
keys, the count pass, a glob search — with timings and RSS).

**Synthetic savepoints** (`tests/common/synth.rs`) are written byte for byte in Flink's layout
(`_metadata` v4, keyed-stream header v6, key-group blocks with the metadata-follows flag, managed
operator state — list, union-list and broadcast states — inline, in files relative to the
savepoint or by absolute `file:` path, or (operator state) as segments of a shared file under a
file-merging handle, optionally Snappy-compressed, Flink's key-group assignment), so file-backed,
multi-subtask and compressed paths are tested **in CI** without Docker. State runs inside a
key-group are written in a non-ascending order by default (`RunOrder::HashLike`), as the heap
backend does (`HashMap` of states): **never assume runs come in stateId order**. Limits: one
storage kind per savepoint (production mixes inline and file handles), and the keyed Snappy
framing is written with the same `snap` crate the reader uses, so only `savepoint-compressed`
proves compatibility with Flink's `SnappyFramedOutputStream`.
`tests/synth_savepoint_test.rs` checks the generated keyed header byte for byte against a golden
one Flink wrote, `tests/operator_state_test.rs` whole operator-state streams against
`savepoint-gitlab` and `savepoint-compressed` (locally). Prefer them for new fixture-free tests.

Gotchas already handled in the pom/IT (don't re-debug):
- **Docker API version**: docker-java defaults to API v1.32 which Docker ≥25 rejects
  ("client version 1.32 is too old"). Fixed by `-Dapi.version=1.43` in the failsafe
  `<systemPropertyVariables>`. Symptom if missing: instant *"Could not find a valid Docker
  environment"* with no logs.
- The savepoint is written to `/tmp/flink-savepoints` **inside** the container (the `flink` uid
  9999 can't mkdir `/savepoints` at root-owned `/`) and copied out with the Docker API, so host
  files are owned by you (a bind mount breaks `mvn clean`).
- The ITs pin `execution.checkpointing.storage: jobmanager` (Flink's default without
  `execution.checkpointing.dir`, see `CheckpointStorageLoader.createDefaultCheckpointStorage`): its
  `MemCheckpointStreamFactory` keeps **every** handle inline → a single self-contained
  `_metadata`, capped at 5 MiB per handle. The inline threshold
  (`execution.checkpointing.data-inline-threshold`, formerly `state.storage.fs.memory-threshold`)
  does nothing there: it only applies to filesystem storage, and is at most 1 MiB. To get state **files**, use
  filesystem storage as `BigSavepointGeneratorIT` does, and copy from both JM and TM.

## Exporting one state (streaming)

`cargo run -- <savepoint> --list-states` lists the keyed states; `--export-state <name>` streams a
single one out (`-o -` for stdout, `--operator <hex prefix>` when a name is ambiguous — timer
states repeat across operators). This is a **headless** path: it never touches the terminal.

`--format ndjson` (default) or `--format parquet`, the latter only in a build with
`--features parquet` (off by default: arrow+parquet add ~36 crates the TUI has no use for; a build
without it says so instead of failing obscurely). Reading is shared — a format is a
`RowSink` (`src/export_state.rs`) and nothing more, so rows are identical across formats and the
tests assert that. The Parquet `value` column is **typed per export**, since one export covers one
state and a state has one value serializer: `Int64`/`Float64`/`Boolean`/`Utf8` for scalars, and
`Utf8` holding compact JSON for anything structured (POJO, Map, List, Tuple, arrays, Kryo). The
`value_type` column carries the Flink serializer type so a reader can tell the two `Utf8` cases
apart. A `value_note` column says why `value` is NULL when the state did hold something
(the decoder's marker, or JSON the typed column cannot hold); NULL `value` with NULL note is a
value Flink stored as null. Files are zstd-compressed, written in 65 536-row row groups.

`src/export_state.rs` reads one state only, so it does not go through the reader's per-key
records (every state of a key, decoded). Instead it reads only each subtask's **header** (it ends where the first
key-group begins) to resolve the state's id/type/serializers, then seeks per key-group via
`KeyGroupsStateHandle.offsets`, reading **one key-group at a time** into a reused buffer. Entries of
other states are skipped by their length prefixes without decoding keys or copying values
(`KeyGroupParse::only_state`). Peak memory is one key-group, not one savepoint —
`ExportStats::max_block_bytes` reports it — plus, for Parquet, the 65 536-row group being built
(~17 MB measured on `savepoint-big`; more with large values).

A key-group that cannot be read to its end mark (truncated, a bad length, a stateId beyond the
header's state count) is **reported, not silently cut short**: the rows read before the defect are
exported, and `ExportStats::malformed_key_groups` / `first_malformed` count and name it — the CLI
prints a `Warning:` on stderr (even with `-o -`), the reader lists it in each page's `malformed`
and the TUI's `i` panel shows it (`Malformed key-groups`).

Two things to know about the rows:
- a **MAP** state exports **exploded**, one row per entry, with `map_key` decoded from the
  namespace (`VoidNamespace + map key`) and the value unwrapped from `NullableSerializer`. The raw
  per-entry bytes are decoded by `decode_map_entry` in `parser::value`, shared with the reader
  (and so the TUI and the `E` export);
- `key_group` is the **absolute** key-group, on this path and on the reader/TUI path alike. Both
  used to report the index *within a subtask's range* (correct only for the subtask starting at 0);
  `extract_keyed_entries` now takes the `KeyGroupsStateHandle` and adds `start_key_group`.
  `tests/key_group_assignment_test.rs` checks the result against Flink's own
  `murmurHash(key.hashCode()) % maxParallelism` and against each subtask's computed range — this
  also fixes the TUI's key-group **range filter**, which could not match subtasks past the first.

## State files: never read a broken one as "no state"

Every **keyed**-state file (relative, absolute — `file:` scheme stripped, remote schemes refused —
or, on the export path, a file-merging segment) is opened through `src/state_file.rs`, which returns an error **naming the path** when the file is missing,
unreadable, or **shorter than the handle's `stateSize`** (`"<path>: truncated: N of M bytes"`;
Flink records the stream position at close, `-1` if unknown, which skips the length check). Do not
go back to `fs::read(..).unwrap_or_default()` or `metadata().ok()?`: that turned a lost file into a
plausible half key set.
- **Reader/TUI**: a subtask that cannot be read is reported by the reader (`KeyedStates.errors`
  from the headers, `Unreadable` in each page that reaches it) and the other subtasks are still
  read. The TUI collects both into the operator's errors: a red `⚠ N state file(s) not read`
  label, never hidden by `hide_empty`, a `⚠ … could not be read: their keys are missing` row after
  its last key, a `Load errors` line under `i` (and a `load_errors` array in the `E` JSON export).
- **Counts cache**: counts that met an unreadable file or a malformed key-group are a lower bound
  (`≥ N`) and are never cached (`App::save_counts`); `--no-cache` neither reads nor writes it.
- **Headless**: `--export-state` fails before writing any row if a file of the target operator
  cannot be read, and also if another candidate operator has an unreadable file (it may hold the
  state too; `--operator` scopes it away). The state is resolved from **every** subtask's header,
  so a bad first file is reported as such, not as "state not found". `--list-states` prints what it
  could read, then every error on stderr, and exits 2. A subtask whose header does not register
  the state contributes no rows (Flink registers states lazily, so subtasks can disagree). The
  output is written to `<path>.partial` and renamed on success, so a failed export never
  truncates a previous one. Operator-state files go through the same checks (`read_whole_stream`):
  a missing or truncated one gives an `[error: <path>: …]` element, never a placeholder.

## StateReader: random access by key-group (`src/reader`)

`reader::StateReader` is the lazy core the web UI (#22/#23) and the TUI migration (#24) build on.
The TUI runs on it (#24, "TUI: on the reader" below). `StateReader::open(dir)` parses `_metadata`
and nothing else; `new(&savepoint, dir)` borrows one
already parsed. The first call on an operator opens and checks each subtask's keyed stream (#16's
`state_file::open`) and resolves its **header** once (`SubtaskHeader`; state ids are per subtask),
one file at a time. Blocks are then read positionally (`read_exact_at` on Unix, `seek_read` on
Windows, `reader/source.rs`) through a `FilePool` of at most `DEFAULT_OPEN_FILES` (64,
`with_open_files`) handles, least recently used closed first — never reopened per key-group.
Inline `ByteStreamStateHandle` bytes are borrowed from `_metadata`, never copied. **Paths from `_metadata` are
not trusted** (`state_file::relative_path` / `open_regular`, used by the reader, the export and the
index): a `RelativeFileStateHandle` path that is absolute or has a `..` component is refused naming
it (`Path::join` would follow it out of the savepoint directory), and a state file is opened only if
it is a regular file — `stat` first, then the handle re-checked — so a FIFO (whose `open` blocks
forever) or a device is refused. Absolute `File`/`Segment` handles may point anywhere local, on
purpose: that is how checkpoints and externalised savepoints reference their files. Then every call
reads whole key-group blocks located by `KeyGroupsStateHandle.offsets`, nothing more:

- `operators() -> Vec<OperatorInfo>` (metadata only), `operator_index(op)` (hex id or unique
  prefix; every call takes `op` as that string), `states(op) -> KeyedStates` (key type; name, type,
  value type, value class, subtask count per state; `errors` naming unreadable files and
  unsupported handles), `value_pojo(op, state)` (a POJO value's layout, for the TUI's labels);
- `key_page(op, &cursor, limit) -> KeyPage { keys, next, stats, malformed, unreadable }`
  (`limit` 0 is taken as 1): each `KeyRecord` has the key, absolute key-group, subtask, its own
  `position`, its entries (`StateEntry`: state, types — `Arc<str>`, shared, not cloned per entry —
  `map_key`, raw namespace, raw value, decoded `value`) and `truncated`. MAP states are one entry
  per map entry (`decode_map_entry`), everything else goes through `decode_state_value`
  (`reader::decode_entry_value`, shared with the export). **Records are bounded**: at most
  `DEFAULT_ENTRY_LIMIT` (1000, `with_entry_limit`) entries per state per key; a state cut there is
  listed in `truncated` (`Truncated { state, total, returned }`), and
  `entries(op, &position, state, offset, limit) -> EntryPage { entries, total, next, .. }` pages
  through all of a key's entries of one state;
- `get(op, key_text) -> Lookup::{Found{record, stats, malformed}, Absent{key_group, subtask},
  NeedsScan{reason}}`: `parser::key_group::JavaKey` parses the text for String/Long/Int/Short/
  Byte/Char/Boolean/Float/Double/BigInteger/Date/LocalDate/Tuple keys, computes Java's
  `hashCode()` (Date/LocalDate verified against JDK 21 output) and Flink's key-group, serializes
  the key and renders it through `decode_key` (so it is compared exactly as the parser renders
  keys), and reads that one block. An empty key-group (offset 0) answers `Absent` with zero I/O.
  POJO/Kryo/opaque/Timestamp keys answer `NeedsScan`, and so does a tuple key whose class (read
  from `TupleSerializerSnapshot`, `extract_key_tuple_classes`) is not Flink's own
  `org.apache.flink.api.java.tuple.TupleN` — a user subclass may override `hashCode()`;
- `scan(op, &cursor, byte_budget, &mut |key| bool) -> ScanPage { matches, next, .., unreadable }`:
  whole blocks until their stored size reaches the budget (at least one block per call), at most
  `DEFAULT_SCAN_MATCH_LIMIT` (1000, `with_scan_match_limit`) matches per call (it stops mid-block,
  `next` at the following key). `scan_with(op, &cursor, &ScanOptions { byte_budget, max_matches,
  last_key_group }, pred)` gives those limits per call and stops after `last_key_group` (a range
  search; `cursor_at(op, kg)` is the cursor a range starts at, named by the subtask holding `kg`);
- `count(op, &cursor, byte_budget) -> CountPage { states: [StateCount { state, keys, entries }],
  keys, next, .. }`: whole blocks from the cursor's key-group (its ordinal is ignored), counting
  per state the keys holding it and its entries (= `--export-state` rows: one per value, per MAP
  entry, per timer), merged by state **name** (ids are per subtask); no value decoded, not cached;
- `progress(op, at, first, last) -> Progress { key_groups_done/total, bytes_done/total }` from the
  offsets alone (no I/O): everything before `at`'s key-group is done; `proto()` exposes the proto
  context so a client (the web download) decodes the same way.

**Errors** are `reader::ReaderError` (`Display`, `std::error::Error`, `From<ReaderError> for
String` so `?` into `String` still works): `Metadata`, `UnknownOperator`/`NoKeyedState`/`NotFound`
(404), `AmbiguousOperator`/`BadCursor` (400), `Unreadable{subtask, first_key_group,
last_key_group, what, error}`/`Malformed{..}` (500). `Cursor::from_str` still errs with a `String`.

**String keys are rendered injectively** (`parser::value::key_string`, used by `decode_key` for a
`String` key or tuple field): a Java string may hold a lone surrogate, which used to become U+FFFD
and merge distinct keys (and make `get` hash the wrong text). Such a string, or one containing a
literal `\u`, renders escaped — lone surrogate `\uXXXX`, backslash `\\` — and every other string as
itself; escaped texts always contain `\u` and plain ones never do. `parse_key_string` inverts it
(`JavaKey::Utf16` holds the units). Values keep their rendering (U+FFFD).

**Order and cursor.** Ascending absolute key-group (= subtask order), and inside a block the order
in which each key first appears on disk. `Cursor { subtask, key_group, ordinal }`, string form
`subtask.keygroup.ordinal` (`1.64.17`), `Cursor::START` = `0.0.0`; a cursor whose key-group belongs
to another subtask is refused. Paging skips empty key-groups using the offsets alone, so `next` is
`None` as soon as no non-empty key-group remains.

**Never stuck, never silent.** What cannot be read is reported in the page and **skipped**: a
subtask whose file is missing/short/unreadable, whose header does not parse, or whose keyed handle
is not a key-groups one (incremental RocksDB, changelog: an `unsupported keyed handle …`
`FailedSubtask`, never silently skipped) is one `Unreadable` covering its key-groups from where the
page reached it; a block that cannot be read or inflated is one `Unreadable` for that key-group.
`next` moves past them and the keys already gathered are kept — a page used to return `Err` at the
same cursor forever. `get` for a key owned by such a unit is `Err(Unreadable)`, never `Absent`. A
malformed block (`BlockDefect`) keeps the keys read before the defect and is listed in
`malformed`; `get` that finds the key there says so in `Found.malformed` (the key may have more
entries past the defect), and one that misses there is `Err(Malformed)`.

**Memory and speed.** One block in flight + an LRU cache of **parsed, grouped** blocks bounded by
approximate weight (`DEFAULT_CACHE_BYTES` 8 MiB, `with_cache_bytes`; a scan does not fill it) +
the page asked for. A block heavier than the whole cache is still kept, alone, so paging through
it does not re-read and re-parse it per page (the cache then holds just that block). Compressed
key-groups are inflated per block. Every result carries `ReadStats` (blocks, cache hits,
`bytes_read` from storage, `block_bytes`, `max_block_bytes`). `StateReader<'static>` is
`Send + Sync`. `export_state_rows` / `list_states` are clients of the same `OpContext` /
`block_range` / `read_block` / `decode_entry_value` (uncached contexts; the export keeps its own
handle per stream and drops each subtask's stream once exported, so it holds one file at a time).

**Measured** on `savepoint-big` (211 MB, 1M keys, 2 files, release): paging all 1M keys with all
their states, 10 000 per page, 3.0 s with the test's own checks, 210.9 MB read (each block once),
peak RSS 38 MB; 50-key pages 0.15 ms each (each key-group read and parsed once for its ~150
pages; it was 9.5 ms when every page re-parsed its block); a cold `get` reads one 1.65 MB
key-group in ~12 ms (`tests/state_reader_big_test.rs`). `--export-state` is unchanged: same bytes,
same time, ~5 MB.

**Limits.** A key's position is only stable for an unchanged savepoint; a key that has several
windows has one entry per window (namespace bytes kept raw); `get` cannot locate POJO keys (their
`hashCode()` is user code) — scan for them. MAP *keys* inside values still render lone surrogates
as U+FFFD.

## Web UI: `--web` (`src/web`, feature `web`)

`flink-explorer <savepoint> --web [--port N] [--bind IP] [--open]` (`--open` spawns
`xdg-open`/`open`/`rundll32 url.dll,FileProtocolHandler <url>` — never `cmd /C start`, which
parses `&` — and reaps the child on a detached thread): a read-only browser UI built
**only on `reader::StateReader`** (never on `src/index`), so it opens at once and reads one
key-group per request. Feature `web` is **on by default** (users take the released binary);
`--no-default-features` is the TUI-only build and CI checks it (`cargo check
--no-default-features --all-targets`). A build without it answers `--web` with an error.

- **Crates**: hyper 1 (`server`, `http1`), hyper-util (`tokio` only — `TokioIo`, `TokioTimer`),
  http-body-util, tokio (`rt`, `net`, `time`, `sync`, `signal`; **no** `macros`, no multi-thread
  runtime: a current-thread runtime is built by hand), getrandom 0.3 (the token). All of
  hyper/tokio is in **`src/web/http.rs`** alone (swap the server there); reader calls run in
  `spawn_blocking`, each holding a permit of a `Semaphore` of `max_concurrent_reads()` (2×CPU,
  2..=64; the permit moves into the blocking closure, so an abandoned request still counts until
  its read ends: 400 parallel big pages used to reach 1.1 GB). At most `MAX_CONNECTIONS` (64)
  connections hold a permit; one more is served by a service answering `web::busy()` (`503`,
  `Retry-After`, security headers, `keep_alive(false)`), never reading the savepoint. Shutdown:
  a `Notify` raced against `poll_accept` with `poll_fn` (no `select!`); `serve` also stops on
  `tokio::signal::ctrl_c`, `start` (tests) does not.
- **Everything else is plain functions**: `App::handle(&Request) -> Response` (`mod.rs`) runs the
  guards then `pages::route`. Guards, in order, for **every** path incl. assets: method
  `GET`/`HEAD` (405 + `Allow`); `host_allowed` (loopback names on any port — `ssh -L` with another
  local port works — plus the bound IP, any IP literal when bound to the unspecified address; a
  DNS name other than `localhost` is refused: that is what rebinding sends); `?token=` → 303 to
  the same URL without it — a **200** HTML page with `<meta http-equiv="refresh"
  content="0;url=…">` + a link, not a 303: a token link followed from another site is a
  cross-site navigation, and a redirect in it may lose a just-set strict cookie; the refresh is a
  navigation of our own origin (meta refresh is not under the CSP) — + `Set-Cookie:
  flink_explorer_<port>=<token>; HttpOnly; SameSite=Strict; Path=/` (port in the name: cookies
  are not port-scoped, which also means every other local port sees it — README says so); else
  the cookie, or 403 without data. Then `SECURITY_HEADERS` (CSP `default-src 'none'; script-src
  'self'; …`, `nosniff`, `no-referrer`, `no-store`, `X-Frame-Options`, `Cross-Origin-Resource-
  Policy` and `Cross-Origin-Opener-Policy` `same-origin`) on every response, the 500 of a
  panicked handler and the 503 included. Token: 16 bytes from
  `getrandom::fill`, compared in constant time. HEAD: body dropped, `Content-Length` kept.
- **Escaping is a type invariant** (`html.rs`): `Html` grows only by `text()`/`display()`
  (escaped) or `raw(&'static str)` (templates). Never add a constructor taking a runtime `String`
  unescaped. No data in `<script>`: `assets/app.js` is static (focus `/` → lookup, expand/collapse all, search continuation, count polling — data only through `textContent`, URLs only
  if they are this server's paths) and every page works without it. Paths from `_metadata` are never served: the only files are
  the two `include_str!` assets.
- **Values**: rendered from the reader's `serde_json::Value` (`<details>` tree, leaves through
  `value_to_display`), raw namespace/value bytes as a hex dump (first 4 KiB). Nothing in `web`
  decodes bytes. Table cells and tree summaries use `pages::preview` / `summary`, which produce
  `value_to_display`'s text **cut while it is built** (a huge value costs the budget, not a
  display string of its whole subtree; unit-tested equal when it fits).
- **Big keys and broken files**: `--web` opens its reader `with_entry_limit(web::ENTRY_LIMIT)`
  (200 per state per record); a key page renders the record only and, per `KeyRecord.truncated`
  state, links to `/op/<id>/entries?at=&state=&offset=` (`StateReader::entries`, paged,
  `limit` default 100) — a state page adds a "more entries" row, JSON twins carry `truncated`
  (with the `more` API link). `KeyPage.unreadable` is shown as a `.warning` (subtask,
  key-groups, file, error) and listed as `unreadable` in the JSON; `Lookup::Found.malformed` and
  `KeyPage.malformed` as errors. A key position inside an unreadable range answers 500 naming the
  file, not 404. `ReaderError` maps to the status (`From<ReaderError> for Fail`):
  UnknownOperator/NoKeyedState/NotFound 404, AmbiguousOperator/BadCursor 400, the rest 500.
  Path segments are decoded with `url_decode_path` (`+` stays `+`); `/op/` (empty prefix) is 404.
- **Routes** (`pages.rs`; search/count/download in `tools.rs`): `/`, `/op/<id>`, `/op/<id>/keys`, `/op/<id>/state?name=`,
  `/op/<id>/key?at=<cursor>` (a page of one from that cursor, which must be that key's own
  position, else 404), `/op/<id>/lookup?key=` (`get`: Found → key view, Absent → key-group and
  subtask, NeedsScan → says so), `/op/<id>/entries?at=&state=&offset=&limit=`,
  `/assets/app.{css,js}`, and `/api/…` twins of each. `limit`
  1..=1000 (default 100). **`/api/` rows are `export_state::row_json`** — the NDJSON line builder,
  shared, so they are byte-identical to `--export-state` rows (`namespace_hex` likewise shared);
  a state page filters the operator's key page, so it may hold fewer rows than `limit`.
- **Operator state** is listed from `_metadata` only (name, mode, subtasks, element count), with
  "use the TUI" for the values: their decoding lives in `src/operators.rs`, used by the TUI.
- **Tests**: `tests/web_test.rs` (CI, `#![cfg(feature = "web")]`) starts `web::start` on
  `127.0.0.1:0` over synth savepoints and speaks raw HTTP/1.1 over `TcpStream`: no/bad cookie or
  token → 403 without data, token → strict cookie + clean redirect, foreign/rebinding `Host` (also
  absolute-form targets) → 403, non-GET → 405, headers on every status, paged `/api/` rows ==
  export rows for every state (inline and compressed files), lookup found/absent/hostile, hostile
  key/value/map key escaped with exactly one (static) `<script>`, shutdown; the login page's
  meta refresh (no `Location`, `//host` → `/`), CORP/COOP, `/op/` 404, an unreadable subtask's
  warning + JSON + 500 key position, a capped key and its entries view (`with_entry_limit(1)`),
  503 past `MAX_CONNECTIONS` idle connections and recovery, parallel requests past the read
  permits all 200. Each guard was
  reverted once to check its test fails.
- **Search, counts, downloads (#23, `tools.rs`, `jobs.rs`)**. `/op/<id>/search` + `/api/…`:
  one `scan_with` per request — `q` 1..=256 chars (`MAX_QUERY_CHARS`), `mode` glob/substring/exact
  (`crate::glob`: the pure matcher moved out of `tui/filter.rs`, which now imports it; `?` is one
  *character*, it used to be one byte), `from`/`to` key-group range, `budget` 1..=64 MiB stored
  bytes (default 4 MiB), `limit` 1..=1000 (default 100), `cursor` (must lie in the range). The
  JSON carries `matches` (`key`, `key_group`, `subtask`, `position`, `href`, `summary`), `next`,
  `next_api`, `next_page`, `done`, `progress` (`Progress` from `_metadata`), `unreadable`. The
  HTML page renders one request and a "Next page of results" link; `app.js` reads `data-api` on
  `#search-status`, fetches `next_api` until `done`, Stop, or 10 000 rows, appending with
  `textContent` only and following only server paths (`ownPath`). A `NeedsScan` lookup links to
  `search?mode=exact` (JSON: `scan`), or — a key longer than 256 chars — to a `substring` search
  of its first 256 (`scan_mode`). Search and count of a non-keyed operator are `NoKeyedState`
  404s; a cursor past the operator's last key-group is a `BadCursor` 400 (`check_cursor`).
  `/op/<id>/count` + `/api/…`: `CountJobs` (one per operator,
  in `App`, kept for the session) run `StateReader::count` steps of `WebOptions::count_step_bytes`
  (4 MiB) on a `std::thread`, checking an `AtomicBool` between steps; a GET starts the job if
  there is none, `?cancel=1` cancels (idempotent; 404 if none) → `cancelling` until the worker
  stops → `cancelled`, `?restart=1` starts again a job whose worker has stopped (polling never
  restarts a cancelled one); at most `WebOptions::max_running_counts` (2) live (running or
  cancelling) jobs over all operators, one more is `Fail::Busy` → 503 + `Retry-After`; a `HEAD`
  (`route(.., side_effects = false)`) only shows a job. Dropping `App` cancels all. Pages
  show `≥ N (counting… x %)` (`tools::count_text`) until `done`; `exact` only when done with no
  unreadable and no malformed key-group (`unreadable_key_groups`/`malformed_key_groups`, all of
  them, not just the 100 listed), otherwise `≥ N (incomplete: k key-group(s) unreadable)`; rows
  carry `data-state` and `app.js` polls `[data-count-api]` once a second (while running or
  cancelling). `/op/<id>/download?state=&format=`:
  `Response.stream: Option<Streamed>` (a `StreamFn` writing to `dyn Write + Send`), produced in
  `http.rs` on a second `spawn_blocking` that holds a **download slot** — its own semaphore,
  `WebOptions::max_downloads` (2), `try_acquire` → `too_many_downloads()` 503 at once — and **no
  read permit** (released when `App::handle` returns), so stalled downloads never starve pages or
  assets. It writes 64 KiB chunks into a bounded `mpsc` (4 in flight, `blocking_send`
  — a slow client back-pressures the export). Stall deadline: `StallWatch` per connection, armed
  when `ChannelBody` hands hyper a chunk and disarmed when hyper polls for the next; armed longer
  than `WebOptions::download_stall` (45 s) → `serve_watched` drops the connection, which drops the
  receiver and ends the producer. The first message decides the status (data → `200`
  chunked, a failure first → a generic `500`, the cause to stderr), `ChannelBody` ends cleanly only on `Chunk::Done`,
  otherwise errs so hyper aborts the connection (no final chunk); a client gone makes the writer
  fail and the export stop. The body is `export_state_rows` + `NdjsonSink` / `ParquetSink`
  (`--features parquet`, else 400): byte-identical to `--export-state -o -`; a malformed key-group
  aborts the transfer. `Content-Disposition: attachment; filename="<8 hex>-<state>.<ext>"`
  (`download_filename`: `[A-Za-z0-9._-]`, no leading dot or `..`, ≤ 100 chars). HEAD drops the
  stream unrun. `WebOptions` (`start_with_options`) exists for tests (`count_step_pause`, the
  limits above).
  **Tests**: `tests/web_tools_test.rs` (CI): a glob/substring search with `budget=1&limit=3` over
  many continuations returns every match exactly once and the same order as one request; the HTML
  chain of next-page links does too; a hostile query and key render escaped; ranges across the
  subtask boundary; parameter bounds (400s); the opaque-key one-click scan; counts == export rows
  (and distinct keys) per state, inline+compressed and files; a throttled job shows `≥`/counting,
  is cancelled and stays so, restarts; NDJSON downloads == `export_state_ndjson` byte for byte
  (multi-chunk, inline+compressed and files), Parquet == `ParquetSink` (with the feature),
  filenames sanitised, a lost file → generic `500` not `200` and a count with `exact: false`,
  403 without session, every key found by lookup and an empty key-group answered with 0 blocks;
  `stalled_downloads_do_not_block_the_server` (more non-reading 4 KiB-`SO_RCVBUF` download
  sockets than read permits: `/api/operators`, assets and a keys page still answer, exactly
  `max_downloads` get `200`, the rest and one more `503`, the stalled ones are aborted after the
  deadline and a new download succeeds — fails on the old permit-holding code), the count cap +
  `cancelling`, HEAD starts no count, the long NeedsScan key. Reverted once each (range stop,
  first-message status, cancel check, mid-block resume ordinal, filename sanitising): each fails.
  **Measured** on `savepoint-big` (release): full-operator search 43 requests 1.6 s, exact scan
  1.6 s, a 2-key-group range 0.3 s, count of all states 1.5 s (1M keys, big-stats 2.5M entries),
  NDJSON downloads 155 MB in 0.8 s and 412 MB in 2.7 s, identical sha256 to the CLI export; peak
  RSS 35 MB through all of it; a cancel stops within one 4 MiB step; an aborted download stops
  the export at once.

## TUI: on the reader, text, terminal

- **No index, no cap (#24).** The TUI used to build an in-memory index of every key and raw value
  (`src/index`, gone; 4–5 s and 1.06 GB on `savepoint-big`, capped at 100 000 keys, all copied
  again by `TreeView::new`, and cached with every raw value in `.flink-explorer-cache` inside the
  savepoint directory). Now `App::open` parses `_metadata`, reads the operator states
  (`src/operators.rs`: operator metadata, `info_lines`, decoded operator-state elements with their
  bytes — operator state is small and read whole) and each operator's keyed **headers**
  (`reader.states`), and draws: nothing else is read before the first frame.
- **`TreeView` holds navigation state only**: per keyed operator a `KeySeq` (the positions —
  reader `Cursor`s — of the keys found so far, the cursor to go on from, the window's offset) and
  the `KeyRow`s of the window ± one page (built once from the reader's `KeyRecord`s: entries per
  state and the value they make, `export::state_value` — a MAP's entries as one object, otherwise
  the first non-empty entry). `ensure_window` asks the reader for what the window lacks
  (`key_page` from a known position: contiguous runs in one call) and drops rows far from it, so
  memory is a few pages, the positions paged through (12 bytes a key) and the reader's 8 MiB block
  cache. A rebuild of the rows (expand, toggle, scroll inside the window) reads nothing:
  `decode_calls()` counts the keys received from the reader. A MAP cut at the reader's entry
  limit (1000) shows its true `total` and is completed with `reader.entries` when expanded, so its
  rows are those of the whole map. A new proto context makes a new reader over the same savepoint.
- **Order.** Keys come in the reader's order — key-groups ascending, Flink's on-disk order inside
  one — not sorted (they used to be, by the index).
- **Counts** are shown as known: `N keys` once counted or paged to the end, `≥ N keys` before,
  `? keys` when nothing is known yet; the pager rows' total likewise (`↓ 1-30 / ≥ 60`). A
  background pass (`tui/jobs.rs`, a worker thread over the shared `Arc<StateReader>`, `scan` with a
  predicate that matches nothing, 4 MiB of blocks a step) counts every operator with progress on
  its row; `C` stops/restarts it, `q`/Ctrl-C/dropping the `App` cancel and **join** it. Complete
  counts (no unreadable or malformed key-group) are saved by `tui/counts.rs` under the user cache
  dir (`$XDG_CACHE_HOME`/`~/.cache` or `%LOCALAPPDATA%`, `std::env` only), one text file per
  savepoint named by an FNV-1a hash of its canonical path: a 3-line header (format, `_metadata`
  size + mtime in ns, path) checked before the body, then `<op hex> <count>` lines. Counts only,
  never a key; never in the savepoint directory. `--no-cache` disables it (`AppOptions`).
- **Filter.** A pattern is searched by `TreeView::search_step` (one `reader.scan` under a byte
  budget per step, `SEARCH_STEP_BYTES` 4 MiB; the event loop runs a step per turn and reads keys
  between them, so `q`, `Esc` — which stops the search and keeps its matches — and Ctrl-C answer
  at once), operator after operator, with `searching N%` on the row. Matches are kept as positions
  (their rows fetched by `key_page(pos, 1)` when shown). An exact key goes through `reader.get`
  (instant, no scan) and falls back to a scan only on `NeedsScan` (POJO keys…); a pattern without
  wildcard is first looked up with `get` (then the scan adds its other cases; matches are
  de-duplicated by position). A key-group range seeks its first key-group's cursor (the subtask
  from `subtask_for_key_group`) and stops past its last.
- **`E` export** streams from the reader on a worker thread (`export::export_json`, through
  `export.json.partial`): every operator and every key, no cap, no `truncated` flag any more.
  The document's per-operator `total_keys`, `load_errors`, `malformed_key_groups` and `info`
  follow its `keys` (they are known once the keys are read).
- **Measured** on `savepoint-big` (release, `examples/bench_tui.rs`, warm page cache): first frame
  after 1 ms at 5 MB RSS (was 4–5 s and 1.06 GB); expand + first page 12 ms; 100 000 keys paged with
  PageDown in 0.8 s (0.5 ms a press, frames 0.15 ms) at 26 MB; the count pass over 1M keys 1.4 s;
  a `*99999*` search over 1M keys 1.6 s; peak 37 MB.
- **Operator-state elements** page like keyed partitions: 500 rows at a time behind `↑`/`↓` pager
  rows (`StateSnapshot::entry_offset`), each row showing the element's **real** index, also under a
  filter (`(12/1200 entries)` in the header). `↓`/`↑` onto a pager row scroll its window by one;
  `PageDown`/`PageUp` move a page (the list's height at the last render) and, at a pager row,
  scroll by the rest of the page (`TreeView::scroll` takes the amount); `G` lands on the last
  `Data` row, never on a pager row.
- **Raw bytes.** `x` (or Enter on a leaf) opens the hex `DetailOverlay` on the selected row's bytes
  (`TreeView::selected_raw_bytes()`): a keyed value — from its state row or any row under it — as
  the reader returns the entry (`StateEntry.raw`; the key panel holds the rendered key, since no
  raw key bytes are kept), under a MAP the entry the row shows (or the first one, on the state's
  row: title `m [Map<String,Long>]: entry 1 of N`, key panel = its namespace, `VoidNamespace` byte
  + serialized map key), or an operator-state element's own bytes (from `OperatorSummary`, the one
  copy). `expand()` returns `false` on a leaf, and the app then opens the bytes.
- **Text.** Shorten display text only through `src/tui/text.rs` (`width`, `truncate_to_width`,
  `ellipsize`): it measures terminal columns and cuts on a `char` boundary. `&s[..n]` on display
  text panicked as soon as `n` fell inside `é`, CJK or an emoji. Widths come from ratatui's own
  `Span::width` (unicode-width), so no extra dependency.
- **Terminal.** `tui::terminal::TerminalGuard` installs a panic hook **before** raw mode is
  enabled (restore the terminal, then chain to the previous hook, so the message prints on the
  normal screen), and its `Drop` restores raw mode, the alternate screen and the cursor on every
  other way out. Ctrl-C (`app::is_interrupt`) quits from any mode: raw mode delivers it as a key
  event, so `App::handle_key` takes the whole `KeyEvent`; it also stops the count pass and the
  search. While the headers load, `main.rs` checks for it at each step (one per operator; exit
  130), and keys typed meanwhile are dropped instead of replayed.

## Decoder architecture — IMPORTANT

**`src/parser/value.rs` is the single source of truth for binary value decoding.** It turns a
state value's raw bytes into a `serde_json::Value`: `decode_state_value(bytes, value_type,
pojo_info, proto)`. The JSON export (`src/export.rs`), the streaming export and the TUI
(`src/tui/tree.rs`) all render this Value; do **not** add a second decoder. To support a new type,
add it here once.

- **TUI.** Every label is `value_to_display` of the decoded Value; a MAP's rows are the decoded
  object's entries (one `StateEntry` per map entry from the reader, `decode_map_entry`),
  a LIST's rows the decoded array's elements, a structure's rows its decoded fields (first 50 map
  entries / 100 elements, then `… +N more`). The TUI only adds presentation: `auto_date` (a `Long`
  in the 2000–2040 epoch-millis range shown as a date — on by default, so compare with the export
  with it off) and the debug hex suffix. It used to have ~900 lines of decoders of its own, which
  disagreed with the export on 18 of 43 states of `savepoint-gitlab` (MAP values read with their
  `NullableSerializer` byte, keyed `ListState` read as length-prefixed, `Map<K,V>` split on the first
  comma, no Byte/Tuple/array/TTL arm). `tests/tui_parity_test.rs` (CI) and
  `tests/tui_gitlab_parity_test.rs` (fixture) render every (key, state) and compare.

- `src/parser/keyed_state.rs` extracts state metadata + the value serializer **type string** per
  state (e.g. `Long`, `Pojo`, `List<String>`, `Map<String,Pojo>`, `Tuple<String,Long>`, `int[]`,
  `Kryo`). The value serializer is the **first** snapshot after the options (not the last — a
  POJO's last *nested field* serializer would mislabel it). POJO info is matched to a state **by
  class name** (`extract_value_pojo_classes`), because `extract_pojo_infos` dedupes by class.
- `src/parser/pojo.rs` parses POJO *structure* (field names/types) from `PojoSerializerSnapshot`.
- Serializer snapshot classes are matched by their **exact** name (`keyed_state::serializer_name` +
  `leaf_serializer_type`, a table shared with `pojo.rs`): a substring match read `BigIntSerializer`
  as `Int`.
- **Keys** are decoded in `value.rs` too (`decode_key`), for the reader/TUI and the streaming export
  alike. The reader groups a block's entries by the rendered key, so the rendering is **injective**:
  String/Long/Int/Short/Byte/Char/Boolean/Float/Double/BigInteger/BigDecimal/Date/LocalDate as
  their value, `Tuple<…>` of those as compact JSON (`["user-1",1]`, strings escaped), POJOs as
  `{ id: 7, name: "x" }`; anything else (Kryo, custom serializers, NaN, …) as `0x` + the hex of the
  key's own bytes (`opaque_key`). `keyed_state::split_key` separates the key from its namespace
  (and a MAP state's map key) with the per-state `EntryLayout` read off the header
  (`extract_entry_layouts`: the namespace serializer's width — VoidNamespace 1, TimeWindow 16,
  GlobalWindow 1 — and the map key type), and keeps a decoded key only when exactly one namespace
  (+ one map key) follows it, which rejects a POJO decode stopped by a Kryo field. Timers render
  their key the same way, so they land in their key's partition.
- Protobuf: a POJO with `className` + `protoBytes` fields is decoded against a `.proto` when a
  profile with a matching `[[proto_patterns]]` is loaded (`src/proto/`). `.proto` files are
  gitignored (project-specific).

### `null` vs "could not decode"
`null` in a decoded value is only ever a null Flink stored (null POJO field/POJO, `NullableSerializer`
null, null `StringValue`, null `GenericArraySerializer` element). What could not be decoded is a
**string marker** instead (`value.rs` module docs; `undecodable`, `not_decoded_after`, `is_marker`):
`"[Kryo]"`, `"[undecodable: <type>]"` (unknown serializer, a POJO subclass, or bytes that end
early), `"[not decoded: follows <field>]"` for every POJO/tuple field after one whose length is
unknown, and `"[not decoded: <n> more elements]"` closing a list cut short. Internally this is
`Decoded = Result<Value, Value>`: `Err` carries the partial value and means the reader position is
lost, which is how a nested POJO's failure stops its parent (it used to read on, from garbage).
`value_to_display` prints markers without quotes, and the TUI shows the decoded Value, so the
same markers, on its state rows and its per-field rows. In a typed Parquet scalar column a marker is NULL
(only truncated bytes produce one there).

### Decoded correctly
Scalars, `char`/`Character` (`Char` → a one-character string) and `char[]` (an array of them; a
lone surrogate is shown as its Java escape, e.g. `\uD83D`, so distinct chars stay distinct), strings with characters outside the BMP (`StringValue` writes
UTF-16 units, so surrogate pairs are decoded together), date/time (`Date`/`Timestamp`/`Instant`;
note: Flink serializes `java.time.LocalDate`/
`LocalTime`/`LocalDateTime` POJO fields with **Kryo**, so they show as `[Kryo]` — the dedicated
`Local*Serializer` formats are still handled in `value.rs` for the explicit-state case, layouts
verified vs Flink: `LocalDate`=int+byte+byte, `LocalTime`=byte+byte+byte+int, `LocalDateTime`=13B),
Enum (as name), `Char`, `BigInteger`/`BigDecimal` (exact, as strings — `BigDecimal` in Java's
`toString()` form, which keeps `1.0` and `1.00` apart), nested POJO, `byte[]`/`String[]`, primitive
arrays
(`int[]/long[]/double[]/…`), Tuples, **keys** of every type above plus Tuple and POJO keys (see
`decode_key`), keyed `ListState` (delimiter-separated), `MapState`
(keys + values, incl. POJO values), protobuf-in-POJO (with a profile), **window state**
(`window-contents`) and **timers** (priority-queue states `_timer_state/...`, shown as the firing
timestamp), **TTL-wrapped values** (`Ttl<T>` → `{ _ttlLastAccess, value }`, via `TtlSerializer`),
and **broadcast maps** (best-effort String key/value). Keyed state is merged across **all** subtasks
(every key reachable, see "TUI: on the reader, text, terminal"), and the key-group parser handles the shared keyed/priority-queue stateId space
(terminating on `END_OF_KEY_GROUP_MARK`). A state's run of entries ends on Flink's
**metadata-follows flag** (`0x80` on `key[0]` of its last entry) and on nothing else — keys of any
length (a 12 KB map key included) parse; the old `keyLen > 10000` heuristic dropped the rest of the
key-group. The header extractors get the **header only** (`&data[..first key-group offset]`) and
scan all of it, with the state metadata computed once: they used to stop at 50 KB, so operators
with hundreds of states lost every state registered past it.

**Kafka connector** state is decoded (`src/parser/kafka.rs`, verified against
https://github.com/apache/flink-connector-kafka): source **reader splits** (`SourceReaderState` →
topic-partition + start/stop offsets), source **enumerator** (operator coordinator state → assigned
partitions + `initialDiscoveryFinished`, shown via `info_lines`), and the sink **writer**
(`writer_raw_states` → `transactionalIdPrefix`) + **committer** (`streaming_committer_raw_states` →
a pending `KafkaCommittable`, decoded precisely: `transactionalId` + `producerId` + `epoch`). The
committer wraps each committable via `SimpleVersionedSerialization` (`[int version][int len][payload]`,
`KafkaCommittableSerializer.getVersion()==1`); we locate that framing inside the committable-collector
bytes and parse `writeShort(epoch)+writeLong(producerId)+writeUTF(transactionalId)`. A committable
appears only if the savepoint was taken with an in-flight transaction — `KafkaSavepointGeneratorIT`
produces continuously (`startContinuousProducer`) to force one. Operator-state elements are framed as
`[int outerLen][int version][int innerLen][payload]`. These decoders receive each element's bytes
from the shared operator-state reader below, so they work on file-backed and compressed streams.

### Operator info
`OperatorSummary::info_lines(keyed)` (`src/operators.rs`; the keyed part — key type, key count,
state names, load errors, malformed key-groups — is what the TUI knows from the reader) aggregates
everything extractable about an operator (OperatorID/uuid,
parallelism, max parallelism, subtasks, coordinator-state presence, key type, keyed-state names,
operator-state names + distribution mode, sizes, fully-finished). Shown via the `i` key in the TUI
and in the JSON export (`info` block per operator; the export includes **all** operators, not just
keyed ones). Operator states are surfaced with their real name + mode (`SPLIT_DISTRIBUTE` / `UNION`
/ `BROADCAST`) and decoded element values.

**Operator-state streams** (`src/parser/operator_state.rs`, verified against Flink release-1.20
`DefaultOperatorStateBackendSnapshotStrategy` / `OperatorBackendSerializationProxy` /
`OperatorStateRestoreOperation` and snappy-java 1.1.10.4) are read wherever the handle's delegate
keeps them — inline, `RelativeFileStateHandle` (joined to the savepoint dir), `FileStateHandle`, or
a `SegmentFileStateHandle` (offsets relative to `startPos`; the file-merging handle, tag 17, is
parsed) — via `export_state::read_whole_stream`, which resolves handles like the streaming export.
A stream that cannot be read gives each state with elements a single `[error: <path>: <why>]`
entry, and an element past the end of its stream an `[error: offset …]` one, never a `name[i]`
placeholder. Layout: header `[int version=6][bool usingStateCompression]` + meta info, then every
list state's elements, then every broadcast map; with compression, **everything after the header is
one** Snappy-framed stream (stream identifier once, right after the header), flushed before every
recorded offset, so each element is its own run of chunks and is inflated on its own
(`inflate_snappy_chunks`, no checksum check, like Flink). An element ends at the next offset of
**any** state (binary search over all offsets). The decoded label is `OperatorElement.label`, and
the element's bytes are kept (`OperatorElement.bytes`) for the TUI's hex view (`x`), **once**: in
the `OperatorSummary` the tree owns. Their total is the elements' size, which is the stream's size
without compression but **more** with it (each element is kept inflated), and a label does not
bound them: an undecoded element's label is `[N bytes] <16-byte hex preview>` whatever N. Only an
element that could not be read has `bytes: None` (label `[error: …]`).

> A savepoint's metadata stores only the **OperatorID** (a hash of the uid). The operator's human
> **name** and **uid** appear only in metadata **v5+** (our Flink 1.20 fixtures are v4 → shown as
> "not in this savepoint's metadata"), and the *operator's* **Java class is never stored**.

### Value classes
Unlike the operator class, a **state value's** fully-qualified Java class (package + name) **is**
stored — in the `PojoSerializerSnapshot` (POJO) / `EnumSerializerSnapshot` (enum). It's parsed into
`CachedPojoInfo.class_name` (incl. nested POJOs, e.g. `com.test.explorer.model.ComplexRecord` with
`…NestedAddress`, and inner classes like `…AllFieldsPojo$DeepNested`), shown in the TUI's value
detail, and exported as `state_descriptors[].value_class`. Scalar values (Long/String/Date/…)
don't store a user class — only the serializer class, which implies the type.

### Known decode limitations
- **Kryo** values are not decoded — marked `[Kryo]`. An undecodable field stops decoding of the
  rest of that POJO, and of the POJOs around it (length unknown): the following fields are marked
  `[not decoded: follows <field>]`. Flink 1.20 serializes `List`/`Map` POJO fields with Kryo too
  (interfaces are a `GenericTypeInfo`), so on the fixture's `AllFieldsPojo` decoding stops at
  `deepNested.innerList`: everything after `deepNested` is marked.
- A **POJO subclass** instance (`PojoSerializer` flags `IS_SUBCLASS`/`IS_TAGGED_SUBCLASS`) is
  written by the subclass's own serializer, which the parent's snapshot doesn't describe: marked
  `[undecodable: subclass …]`.
- `MapSerializer` **POJO fields** (explicit `MapTypeInfo`) are not decoded (`[undecodable: Map<…>]`).
- The **window namespace** itself (TimeWindow start/end) isn't surfaced as a field — only the
  window's buffered contents and its firing timer are shown; if one key has multiple windows, the
  TUI and the `E` export show the first window's contents (the reader and `--export-state` keep
  one entry per window).
- **Snapshot compression** (keyed state) is handled on **both** paths: `extract_keyed_entries`
  reads `usingKeyGroupCompression` from the header — which is never compressed — and inflates each
  key-group through `key_group_block`, so the reader/TUI and the streaming export both work on a
  compressed savepoint. Exercised by the real `savepoint-compressed` fixture, generated by
  `SavepointGeneratorIT.generatesCompressedSavepoint` (pass `compress` to `ClusterJob`, which calls
  `ExecutionConfig.setUseSnapshotCompression`). **Operator** state's own `usingStateCompression` is
  read too (see *Operator-state streams* above). **Raw** operator state (bytes the user wrote,
  with no header to go by) is still listed as `raw-operator-subtaskN` with `name[i]` entries.
- **Broadcast / operator-state element decoding is best-effort String**: broadcast maps decode when
  the key/value are Strings (the common case; `decode_broadcast_map`), and operator `ListState`
  elements decode as `StringValue`; other element types fall back to a byte preview (the operator
  backend's element serializers aren't parsed). This holds for inline, file-backed and compressed
  streams alike.
- **Channel state** (`input_channel_state` / `result_subpartition_state`, unaligned checkpoints) is
  parsed but not displayed — it does not appear in canonical savepoints anyway.
- RocksDB incremental / changelog state is not decoded (canonical heap savepoints only).
- **Keys**: `Timestamp`/`Instant`/`LocalTime`/`LocalDateTime` keys render as hex (their display
  drops sub-millisecond precision, which would merge keys). A `Float` key renders through `f64`
  (`1.100000023841858`). An undecodable key under an unknown (custom) namespace serializer keeps
  its namespace in the hex, and one in a MAP state whose map key is undecodable too keeps the map
  key — distinct keys still never merge, but such a key shows once per namespace / map entry. POJO
  keys keep their `{ field: value }` display, which does not escape strings.

## Binary format references (verify against apache/flink)

**When implementing/changing any byte-level decoding, verify the format against the Apache Flink
source** (branch `release-1.20`, https://github.com/apache/flink) rather than guessing. Raw files:
`https://raw.githubusercontent.com/apache/flink/release-1.20/<path>`. Key references (verified):

- **Per-key-group layout** — `flink-runtime/.../state/FullSnapshotAsyncWriter.java`: keyed states
  **and** priority-queue states (timers) share **one `short` stateId space** —
  `writeShort(kvStateId())` precedes each state's entries; the group ends with
  `writeShort(END_OF_KEY_GROUP_MARK)`. Each entry is `BytePrimitiveArraySerializer.serialize(key)`
  then `serialize(value)` (each = int length + bytes). The last entry of each run has
  `FIRST_BIT_IN_BYTE_MASK` set on `key[0]` (`setMetaDataFollowsFlagInKey`, `:177`/`:217`), and the
  next `short` is written — and read back, `FullSnapshotRestoreOperation.java:301-308` — only
  after such an entry. Never delimit runs by key length or by a bounds check.
- **End marker** — `flink-runtime/.../state/FullSnapshotUtil.java`: `END_OF_KEY_GROUP_MARK = 0xFFFF`,
  `FIRST_BIT_IN_BYTE_MASK = 0x80`.
- **Keyed header** — `KeyedBackendSerializationProxy.write`: `int version`, `boolean compression`,
  key serializer snapshot, then `writeShort(numStates)` (`:133`) and one `StateMetaInfoSnapshot` per
  state (`writeUTF(name)`, `writeInt(backendStateType)`, options, serializer snapshots:
  `StateMetaInfoSnapshotReadersWriters.java:109-125`). That count bounds the key-group stateIds.
- **Timers** — `flink-streaming-java/.../api/operators/TimerSerializer.java`: a timer serializes as
  `writeLong(MathUtils.flipSignBit(timestamp))` + key serializer + namespace serializer. Timers are
  a priority-queue state in the shared stateId space (so they shift keyed-state stateIds — e.g. a
  window operator's `window-contents` is stateId 1 because the timer PQ is stateId 0). To read the
  real timestamp, flip bit 63 back (`ts ^ Long.MIN_VALUE`).
- **List state** — `ListDelimitedSerializer`: elements serialized back-to-back **separated by the
  delimiter byte `,` (0x2c)**, no length prefix (verified empirically: 600 `long`s spaced by 1000).
  Distinct from `ListSerializer` (used for a List *field* inside a POJO) = int length + elements.
- **Map value** — wrapped by `NullableSerializer`: a leading `isNull` byte (0x01 = null), then the
  value. Map key bytes are `VoidNamespace(1 byte) + serialized key`.
- **VoidNamespace** = 1 byte; a window's namespace is a `TimeWindow` = two `long`s (start, end).
- **Entry key** — `flink-runtime/.../state/SerializedCompositeKeyBuilder.java` (used by
  `HeapKeyValueStateIterator`): `kgPrefix + key + namespace`, then the map user key for a MAP
  state. No length is written in between unless key **and** namespace are both variable-sized
  (`isAmbiguousCompositeKeyPossible`), which VoidNamespace (`getLength() == 0`, writes one byte),
  TimeWindow (16) and GlobalWindow (1) never are. In the header, each state's block ends with its
  namespace serializer's snapshot (`writeUTF(class) + int version`, no payload), timers included
  (`TimerSerializerSnapshot` nests `[key, namespace]`).
- **Scalars** — `typeutils/base/`: `ShortSerializer` writeShort, `ByteSerializer` writeByte,
  `CharSerializer` writeChar (2 bytes), `Float`/`DoubleSerializer` writeFloat/writeDouble.
  `BigIntSerializer`: `int len` (0 = null, 1/2/3 = the `ZERO`/`ONE`/`TEN` fast paths, else `len - 4`
  bytes of `toByteArray()`, two's complement); `BigDecSerializer`: that BigInteger (unscaled) then
  `int scale`.
- **Tuple** — `TupleSerializer`: the fields back to back, no null flags. `TupleSerializerSnapshot`:
  composite, outer version 2, outer snapshot `writeUTF(tupleClass)`, one nested snapshot per field.
- **Enum** — `EnumSerializerSnapshot`: `writeUTF(enumClass)` + `int(n)` + `n × writeUTF(constant)`
  (constants in ordinal order).
- **String** — Flink `StringValue` (`flink-core/.../types/StringValue.java`): var-int length =
  `charCount + 1` (`0` = null — `StringSerializer` writes null Strings), then each **UTF-16 unit**
  var-int (`charAt`, so a non-BMP char is two surrogate units).
- **Char** — `CharSerializer` = `writeChar`: 2 bytes, high byte first. `char[]`
  (`CharPrimitiveArraySerializer`) = `int` length + `writeChar` per element.
- **POJO** — `PojoSerializer.serialize`: flags byte (`IS_NULL`=1, `NO_SUBCLASS`=2, `IS_SUBCLASS`=4
  → `writeUTF(class)`, `IS_TAGGED_SUBCLASS`=8 → `writeByte(tag)`), then, for `NO_SUBCLASS`, per
  field `writeBoolean(isNull)` + the field; fields in name order (`PojoTypeInfo` sorts them).
- **Operator-state stream** — `DefaultOperatorStateBackendSnapshotStrategy.java:165-212` +
  `OperatorBackendSerializationProxy.java:97-103`: `writeInt(6)`, `writeBoolean(usingStateCompression)`
  and the meta info, never compressed; then **one** `CompressibleFSDataOutputStream` (Snappy-framed when
  compressed) whose `getPos` (:43-49) flushes before each recorded offset. Restore:
  `OperatorStateRestoreOperation.java:182-267`. A file-merging handle (tag 17,
  `MetadataV2V3SerializerBase.java:624-636`) adds two UTF dirs + an `isEmpty` boolean before its
  `SegmentFileStateHandle` delegate.
- Magic numbers / metadata layout: see `docs/magic-numbers.md`, `docs/parsing-format.md`.

## Tests
- `cargo test` — needs `tests/fixtures/savepoint-gitlab/` (run `mvn verify` first). Note that
  `.github/workflows/ci.yml` runs an **explicit list** of fixture-free tests (add new ones to it;
  `tests/synth_savepoint_test.rs`, `tests/operator_state_test.rs`, `tests/key_types_test.rs` and
  `tests/keyed_block_test.rs` are the fixture-free home for file-backed / multi-subtask /
  compressed, operator-state, key-type and block-level cases, `tests/tui_*_test.rs` for the TUI
  over ratatui's `TestBackend`, `tests/state_reader_test.rs` and `tests/key_group_test.rs` for the
  reader and the key-group assignment, `tests/web_test.rs` and `tests/web_tools_test.rs` for `--web`), so the fixture-dependent ones
  (`gitlab_savepoint_test`, `decode_e2e_test`, `coverage_audit`, `export_state_stream_test`,
  `key_group_assignment_test`, `compressed_savepoint_test`, `export_parquet_test`,
  `tui_gitlab_parity_test`, `state_reader_fixture_test`, `state_reader_big_test`) only ever run
  locally.
- `tests/tui_parity_test.rs` (CI) — every (key, state) of a synthetic savepoint (`Map<String,String>`
  with a null value and a comma, `Map<String,Long>`, `Map<Enum,Boolean>`, `List<Long>` with 0x2c
  bytes in the values, `List<String>`, `byte[]`, `int[]`, `Byte`, `Tuple2`, `Char`, `Long`,
  `Ttl<Long>`, and a key holding none of them) rendered through `TreeView` + `TestBackend` equals
  `value_to_display` of the `--export-state` value, MAP/LIST rows included; the exported values are
  the written ones (the TTL'd one `{ _ttlLastAccess, value }`); `auto_date` only touches `Long`s;
  and a rebuild reads no key again (a new proto context reads the shown ones again). Helpers in
  `tests/common/tui.rs` (a key is isolated with the Exact filter, i.e. `reader.get`), and
  `tests/common/keyed.rs` reads an operator's keyed state whole through the reader (no entry
  limit) for tests that look keys and values up — what the index used to give them.
  `synth::Ser::Ttl(&inner)` writes a `TtlSerializerSnapshot` (composite, outer version 2, nested
  `[LongSerializer, inner]`) and `Val::Ttl(ts, v)` a `TtlValue` (`writeLong(ts)` + the value), per
  `TtlStateFactory.java` (release-1.20).
- `tests/tui_gitlab_parity_test.rs` — the same over every (key, state) of `savepoint-gitlab`
  (2262 pairs, 66 states incl. timers and windows).
- `tests/tui_raw_bytes_test.rs` (CI) — `x` on a keyed value gives its bytes (and the overlay shows
  them), on a MAP state and on a map entry's row that entry (namespace + map key, value behind
  its null byte); an operator state of 1,200 elements pages to `[1199]` (with its
  bytes) and keeps real indices under a filter; keys and elements whose labels look like pager
  rows (`↓ 1-30 / 99`) do not trap the cursor; `PageDown`/`PageUp` cross 1,200 elements (and 99
  keys) a page at a time; `G` lands on a data row; the MAP overlay title fits 80 columns.
- `tests/decode_e2e_test.rs` — protobuf, tuples, primitive arrays, full-POJO, scrollbar, and
  `AllFieldsPojo` field by field (chars decoded, then explicit markers — no `null`) on the reader,
  streaming and `deserialize_pojo_to_json` display paths. It does **not** check the TUI: the
  `tui_*parity_test`s do.
- `tests/value_decode_test.rs` (CI) — `parser::value` on hand-written Flink bytes: `Char`, `char[]`,
  an emoji string, null Strings, every marker (Kryo, unknown serializer, truncated bytes, nested
  POJO, list, tuple, subclass, map key) and their TUI rendering. Reader-level `StringValue` and
  `readChar` cases are in `tests/java_deser_test.rs`; `synth_savepoint_test.rs` runs `Char`,
  `char[]`, `List<Char>`, `Map<String,Char>` and emoji keys/values through both read paths.
- `tests/coverage_audit.rs` — dumps the full operator/state inventory + decoded values (diagnostic).
- `tests/gitlab_savepoint_test.rs` — exhaustive per-state assertions, incl. every state's
  namespace width / map key type as read off the headers Flink wrote.
- `tests/key_types_test.rs` — fixture-free: Short, Double (±0, NaN, ±∞), Boolean, BigInteger,
  Tuple2, Byte, Float, Char and undecodable keys in VALUE and MAP states (plus timers) stay
  distinct on the export and the reader, with their values, Flink's key-group, and the map key in
  `map_key`; BigInteger/BigDecimal/Char layouts; JDK `hashCode()` known answers.
- `tests/export_parquet_test.rs` — **needs `--features parquet`**: the file is well-formed, the
  `value` column is typed per state, map rows keep `map_key`, key-groups are absolute, columns are
  zstd, and the rows read back equal the NDJSON ones. Verified by reading with arrow-rs, not with
  a third-party engine (DuckDB/Polars were not available here).
- `tests/compressed_savepoint_test.rs` — needs `savepoint-compressed`: the fixture really sets
  the compression flag, the reader path inflates its key-groups (same key sets as the uncompressed
  run), the streaming export agrees with the reader path on those bytes, and the operator states
  (`"user-bob"`, the broadcast map) decode like the uncompressed run's, in the operator summaries
  and the JSON export.
- `tests/operator_state_test.rs` — fixture-free (CI): operator-state streams decode the same
  inline, relative / absolute file, segment, and compressed (incl. an element over 64 KiB, i.e.
  several `0x00` Snappy chunks); a missing or truncated stream file shows `[error: …]` entries;
  1M elements are summarized (`operators::summarize`) in well under a second (release; 200k in debug) and time grows linearly.
  Locally it also checks synth's operator streams byte for byte against both real fixtures.
- `tests/key_group_assignment_test.rs` — the reported key-group is the one Flink assigned
  (`parser::key_group`: `MathUtils.murmurHash` + `KeyGroupRangeAssignment`), and every streamed row
  falls inside its subtask's range. POJO keys are range-checked only: their `hashCode()` is user
  code.
- `tests/key_group_test.rs` (CI) — `parser::key_group` against `hashCode()`s and key-groups printed
  by Java itself, and against synth's independent port on every key type (incl. NaN, ±∞, lone
  surrogates, 128-bit BigIntegers), with `JavaKey` serializing to synth's exact bytes.
- `tests/state_reader_test.rs` (CI) — `StateReader` on synth savepoints (inline, compressed,
  relative and absolute files; 3 and 4 subtasks; maxParallelism 128 and 4096; per-subtask state
  ids): paging visits every key once and equals `export_state_rows` row for row; `get` finds
  every key reading exactly its block's stored bytes; absent keys in empty key-groups cost 0
  bytes; 1-byte-budget scans resume without duplicates or misses; cursors round-trip; every
  hashable key type (Date/LocalDate included); NeedsScan (also for a user tuple class); a deleted
  file, a block that does not inflate and an incremental keyed handle are each reported once in
  the pages, which go on past them, and are an `Err(Unreadable)` for `get`; `Found` in a
  malformed block carries `malformed`; a key with 5 000 MAP entries is bounded at the entry limit
  and continued by `entries`; lone-surrogate String keys stay distinct and round-trip through
  `get` (alone and in a tuple); `limit` 0; the open-file bound; concurrent `get`s.
- `tests/state_reader_fixture_test.rs` — the same parity on `savepoint-gitlab` and
  `savepoint-compressed` (real Flink key-group assignment: 675 keys found by `get`, 5 POJO keys
  by scan). `tests/state_reader_big_test.rs` — `savepoint-big` if present (skipped otherwise):
  every key reachable, bounded RSS, timings with `--release -- --nocapture`.
- `tests/export_state_stream_test.rs` — streaming export: strict parity with the full JSON export
  for every non-MAP state, exploded parity for MAP states, the `v6`/compression header, the
  bounded peak block, and no key-group of a real fixture (plain or compressed) reported malformed
  on either path.
- `tests/keyed_block_test.rs` (fixture-free, CI) — block-level parsing on hand-built blocks: a
  Snappy-framed key-group, the state filter, keys of 10,000/10,001/12,000 bytes with 1- and 2-byte
  prefixes (kg 32767), a 12 KB MAP user key mid-run, a > 256 KiB four-state block with timers,
  every `BlockDefect` (truncation, missing end mark, bad lengths, unknown stateId) and the
  header-declared state count as the stateId bound.
- `tests/synth_savepoint_test.rs` (fixture-free, CI) — synthetic savepoints: the header against a
  real one, file-backed/inline/compressed parity, every subtask on the reader, long keys and long
  map keys on both paths (maxParallelism 32768), a > 50 KB header (321 states) on `list_states`,
  the reader and the export, the header read from the header only (a state value that spells a
  state record is not a state), one-letter state names located at their own record, and a
  corrupted key-group reported by the export, the reader and the CLI.
- `tests/state_file_errors_test.rs` — fixture-free (CI): a 2-subtask file-backed synthetic
  savepoint with one state file deleted, truncated or (unix) `chmod 000`. The reader reports an
  error naming the file and keeps the other subtask's keys; the TUI keeps and flags the operator
  (`≥ N keys` once counted, a "keys are missing" row after the last key); `export_state_ndjson`,
  `list_states`, `find_state` and the CLI (exit code) fail naming the file; no counts are cached
  after a partial count, complete ones are (in the cache dir, nothing in the savepoint's).
- `tests/tui_cap_test.rs` (fixture-free, CI) — no cap: 150 000 keys (more than the old 100 000)
  over 2 subtasks paged with PageDown to the very last key (subtask 1's) and back, keys of both
  subtasks shown, the pager's total exact at the end (`↑ 149971-150000 / 150000`); `↓` row by row
  shows every key once in the reader's order; 3 and 4 subtasks all reachable; the `E` export
  (`write_json`) streams every key (valid JSON, no `truncated`, cancellable) and the app runs it in
  the background; a tree over a subset of operators.
- `tests/tui_search_test.rs` (fixture-free, CI) — a glob is scanned step by step (progress on the
  row, every match of both subtasks); an exact String or Long key is found by `get` with no scan
  (one key read), an absent one at once, a pattern without wildcard shown before the scan and not
  twice; a key-group range across the subtask split; Ctrl-C stops (and joins) the count, `C`
  toggles it; Ctrl-C and `Esc` stop a search; opening, paging, filtering and counting change
  nothing in the savepoint directory, the counts file holds counts only, is reused, and is stale
  once `_metadata` changes; `--no-cache` and `XDG_CACHE_HOME`/`HOME` give the cache dir.
- `tests/tui_text_test.rs` (fixture-free, CI) — `TestBackend` renders of long non-ASCII values (é,
  CJK, Greek, `☕`) at 80 and 120 columns, with and without debug labels, and of the loading screen
  with a non-ASCII operator name; Ctrl-C quits from the tree, the filter input and the overlays.
  The width/truncation unit tests (every width, 4-byte emoji included) live in `src/tui/text.rs`.
- `tests/terminal_guard_test.rs` (fixture-free, CI) — the panic hook restores the terminal before
  the previous hook prints, including for a panic inside `enter` (i.e. right after raw mode), and
  restores exactly once. One test only: panic hooks are process-global.

## Dependencies — IMPORTANT

**No unmaintained crates.** When touching dependencies, run `cargo audit` and resolve **every**
advisory, including `unmaintained`/`unsound` *warnings* (not just vulnerabilities). The tree must
stay clean.
- Prefer **removing** a dep if it's unused (`tui-tree-widget` and `dirs` were dead and were dropped;
  removing `tui-tree-widget` also deduped a second `ratatui`).
- The value-holding index cache and its `postcard` dependency are gone (#24). The counts cache
  (`src/tui/counts.rs`) is plain text written with `std` alone; do not add a serialization crate
  for it (and never `bincode`: unmaintained, RUSTSEC-2025-0141). Bump its `MAGIC` line if its
  meaning changes: a mismatching header is simply ignored.
- Keep `ratatui`/`crossterm` current (≥0.30/0.29) — older `ratatui` pulls `paste`/`lru` advisories.

<!-- MANUAL ADDITIONS END -->
