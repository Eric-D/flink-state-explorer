# flink-explorer

[![CI](https://github.com/Eric-D/flink-state-explorer/actions/workflows/ci.yml/badge.svg)](https://github.com/Eric-D/flink-state-explorer/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Eric-D/flink-state-explorer/actions/workflows/codeql.yml/badge.svg)](https://github.com/Eric-D/flink-state-explorer/actions/workflows/codeql.yml)
[![Security audit](https://github.com/Eric-D/flink-state-explorer/actions/workflows/audit.yml/badge.svg)](https://github.com/Eric-D/flink-state-explorer/actions/workflows/audit.yml)
[![Release](https://img.shields.io/github/v/release/Eric-D/flink-state-explorer?include_prereleases&sort=semver)](https://github.com/Eric-D/flink-state-explorer/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable%20(edition%202021)-orange.svg)](https://www.rust-lang.org)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)

> **Status:** provided as-is — issues and pull requests may not be actively monitored.

TUI tool for exploring Apache Flink 1.20 **canonical** savepoints interactively — without a JVM.

Browse operators → keyBy values → states → values from a savepoint directory. Decodes POJO fields
inline, expands MAP entries, resolves enum names, reads Kafka connector state, and decodes protobuf
payloads. Everything visible in the TUI is also available as a JSON export.

## Features

- **Flink 1.20 canonical savepoints** (heap / HashMap backend) — metadata versions 3–5.
- **Key-first navigation** — Operator → keyBy value → state entries, merged across **all** subtasks.
- **Broad value decoding** (single decoder shared by the TUI and the JSON export):
  - scalars (`Int`/`Long`/`Short`/`Float`/`Double`/`Boolean`/`Char`/`String`, emoji included),
    date/time (`Date`/`Timestamp`/`Instant`; `java.time.Local*` are Kryo-serialized by Flink →
    shown as `[Kryo]`),
  - `Enum` (resolved to the constant **name**, not the ordinal),
  - `byte[]`, `String[]`, primitive arrays (`int[]`/`long[]`/`char[]`/`double[]`/…), `Tuple<…>`,
  - **recursive POJOs** (nested POJOs and inner classes) rendered as inline JSON,
  - **keyed `ListState`**, **`MapState`** (keys + values, incl. POJO values),
  - **TTL-wrapped** values (`Ttl<T>`), **broadcast** maps, **window contents**, and **timers**
    (shown as the firing timestamp).
  - What cannot be decoded is **marked, never `null`**: `[Kryo]`, `[undecodable: <type>]`, and
    `[not decoded: follows <field>]` for the fields after it (Flink writes no field lengths).
    `null` always means Flink stored a null.
- **Kafka connector state** 📨 — source reader splits (topic-partition + offsets), source enumerator
  (assigned partitions), sink writer (`transactionalIdPrefix`), and sink committer (a pending
  `KafkaCommittable`: `transactionalId` + `producerId` + `epoch`). Kafka operators/states are flagged
  with a 📨 marker in the tree.
- **Protobuf-in-POJO decoding** — detect `{className, protoBytes}` patterns and decode with `.proto`
  files configured per profile.
- **Operator info** (`i`) — OperatorID/uuid, parallelism, max parallelism, subtasks, coordinator
  state, keyed/operator state names + distribution mode, sizes.
- **Value class** — a state value's fully-qualified Java class (package + name) is surfaced when the
  serializer stores it (POJO/enum).
- **JSON export** (`E`) — streams every operator and every key, with its decoded values, to
  `export.json` next to the savepoint (in the background; `--export-state` streams one state).
- **Instant start, every key** — the TUI reads a savepoint lazily, a key-group at a time: the
  first frame is up once `_metadata` and the state headers are read, every key of every subtask
  is reachable (no key cap), and memory stays at a few pages of keys plus a small block cache (a
  211 MB / 1M-key savepoint: first frame in a few milliseconds, 100 000 keys paged in under a
  second at ~26 MB).
- **Filtering** — glob pattern (searched step by step, cancellable), exact key (looked up
  directly by its key-group), or key-group range.
- **Key counts** — counted in the background (`≥ N keys` until then), cached under the user cache
  directory (counts only, never in the savepoint directory).
- **Web UI** (`--web`) — the same savepoint in a browser, read-only, loopback-only, token-guarded;
  see [Web UI](#web-ui).
- **Profiles** — per-project proto dirs, patterns, and bindings.

## Build

Requires Rust stable (edition 2021). The default build is **pure Rust** — no C/C++ toolchain
needed, so it cross-compiles cleanly (e.g. to Windows with `x86_64-pc-windows-gnu` + mingw-w64):

```bash
cargo build --release
```

Reading external RocksDB/SST state files is optional and off by default (it pulls in the native
`rocksdb` crate, which needs a C/C++ compiler + `libclang`):

```bash
cargo build --release --features sst
```

The [web UI](#web-ui) (`--web`, hyper + tokio) is a default feature; `--no-default-features`
builds the TUI-only binary.

### Prebuilt binaries

Each tagged release publishes Linux (`x86_64`), Windows (`x86_64`), and macOS (`arm64`)
binaries to **GitHub Releases** — see the
[Releases page](https://github.com/Eric-D/flink-state-explorer/releases).

## Quick Start

```bash
# Open a savepoint
./target/release/flink-explorer /path/to/savepoint-dir

# Open with a specific profile (for proto decoding)
./target/release/flink-explorer -p my-profile /path/to/savepoint-dir

# Neither read nor write the key counts cache
./target/release/flink-explorer --no-cache /path/to/savepoint-dir
```

`flink-explorer --help` lists the flags: `-p/--profile <name>`, `--no-cache`.

A state file that is missing, unreadable or shorter than `_metadata` says is never taken for
"no state": the operator is flagged `⚠ N state file(s) not read` (details under `i`), the other
subtasks are still read, the key list says where keys are missing, and counts that met such a
file are not cached. `--list-states` and `--export-state`
fail with the file's path and a non-zero exit code.

## TUI Layout

```
┌ flink-explorer — savepoint-dir (checkpoint 114045) ──────────────────────┐
│  ⯈ [rocheteau] Suivi des joueurs des Verts (1 keys, 7 states)            │
│    ⯈ rocheteau (7 states)                                                │
│      ⯈ buts-par-saison [Map<String,Int>]: 15 entries                     │
│      • matchs-joues: 416                                                 │
│      • date-blessure: null                                               │
│▶     ⯈ fiche-joueur: { nom: "Rocheteau", titulaire: true, ... }          │
│          nom: "Rocheteau"                                                │
│          titulaire: true                                                 │
│  ⯈ 📨 6f007a2e… (1 states)        ← Kafka sink operator                  │
│    ⯈ 📨 [internal] writer_raw_states [operator/UNION] (1 entries)        │
│      • transactionalIdPrefix=verts-txn                                   │
├──────────────────────────────────────────────────────────────────────────┤
│ [rocheteau] Suivi des joueurs > rocheteau > fiche-joueur.nom             │
│ q:quit ↑↓:nav Enter:expand x:hex Esc:collapse /:filter i:info E:export   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Tree Levels

| Level | Content | Example |
|-------|---------|---------|
| **Operator** | Flink operator (📨 if Kafka) | `[rocheteau] Suivi des joueurs des Verts (1 keys, 7 states)` |
| **Partition** | keyBy value, in the savepoint's order (below) | `rocheteau (7 states)` |
| **State entry** | State name + decoded value | `matchs-joues: 416` |
| **Expanded** | POJO fields / MAP pairs | `nom: "Rocheteau"` |

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `j` / `↓` | Move down |
| `k` / `↑` | Move up |
| `PageDown` / `PageUp` | Move a page (the tree's height) down / up; past a `↓`/`↑` row, the keys or operator-state elements scroll by the rest of the page |
| `l` / `Enter` / `→` | Expand; on a value or an operator-state element, open its hex detail |
| `x` | Hex detail of the selected value (keyed value, MAP entry, operator-state element): its raw bytes |
| `h` / `Esc` / `←` | Collapse / clear filter / close overlay |
| `g` / `Home` | Jump to first |
| `G` / `End` | Jump to the last row (of the keys or elements shown: `↓` past it scrolls) |
| `i` | Operator info (OperatorID, parallelism, state names, …) |
| `E` | Export every operator and key, decoded, to `export.json` (in the background) |
| `C` | Stop / restart the background key count |
| `S` | Savepoint summary |
| `I` | List loaded `.proto` files |
| `/` | Open the filter bar (Tab cycles pattern / exact / key-group); `Esc` stops a search |
| `P` | Profile picker |
| `H` | Toggle hiding empty operators |
| `D` | Toggle debug mode (extra type tags, key-groups) |
| `q` / `Ctrl-C` | Quit (`Ctrl-C` also from the filter bar and overlays, during a search or the key count, and while loading — then the process exits with 130; once loaded, quitting is a normal exit, 0) |

In the detail overlay: `Tab` switches the key/value panel, `↑`/`↓` scroll, `Esc` closes. The value
panel holds the entry's bytes as the savepoint stores them. Under a MAP state each entry is its
own: on a map entry's row (or the first entry, on the state's row) the key panel holds its
namespace (`VoidNamespace` byte + serialized map key) and the value panel the value behind its
`NullableSerializer` null byte. Elsewhere the key panel holds the key as rendered (raw key bytes
are not kept).

### Key order, counts and search

Keys are listed in the order the savepoint stores them: **key-groups ascending**, and inside one
key-group in **Flink's on-disk order** (the order the heap backend wrote them, not sorted). The
TUI reads the page it shows from the savepoint (`src/reader`), so every key of every subtask can
be reached, whatever their number.

An operator shows `≥ N keys` until it is counted: a background pass counts every key (progress on
the operator row; `C` stops or restarts it; `q`/`Ctrl-C` stop it). Complete counts are cached in
`$XDG_CACHE_HOME/flink-explorer` (or `~/.cache/flink-explorer`; `%LOCALAPPDATA%\flink-explorer`
on Windows), one small file per savepoint holding counts only, ignored once `_metadata` changes.
Nothing is ever written into the savepoint directory except by `E` and `--export-state`.

The filter bar's **pattern** (`*` and `?` wildcards, case-insensitive) is searched through every
key a few key-groups at a time: matches show as they are found, with the search's progress, and
`Esc` stops it (keeping what it found). An **exact** key of a String, numeric, Boolean, Char,
BigInteger, Date, LocalDate or Flink-tuple key type is looked up directly — its key-group computed
with Flink's hash, one block read; a pattern without wildcard shows that key at once, then
searches for its other cases. Other key types (POJOs) are searched. A **key-group range** pages
from its first key-group to its last.

## JSON Export

Press `E` in the TUI to write `export.json` into the savepoint directory (through
`export.json.partial`, renamed when complete). It mirrors the TUI model and includes **all**
operators (not just keyed ones): per operator its `operator_states`, its `state_descriptors` (with
`value_class` when known), **every key** with its decoded values (`{key, key_group, states}`), then
`total_keys`, `load_errors`, `malformed_key_groups` and an `info` block (these follow the keys:
they are known once the keys are read). It is streamed from the savepoint a page of keys at a
time, on a background thread, so memory stays bounded whatever the savepoint's size. A MAP state
is one object of all its entries; another state holds the value of its first entry (a key with
several windows or timers: the first one, as in the TUI). The export and the TUI share the same
decoder (`src/parser/value.rs`), so they never diverge. To export one state as NDJSON or Parquet,
use `--export-state`.

## Web UI

For people who would rather click than use a terminal, the same binary serves a read-only
browser UI:

```bash
flink-explorer /path/to/savepoint-dir --web            # prints the URL to open
flink-explorer /path/to/savepoint-dir --web --open     # …and opens it in the default browser
flink-explorer /path/to/savepoint-dir --web --port 8080
```

It prints one URL, `http://127.0.0.1:<port>/?token=<32 hex digits>`; open it, and the token is
swapped for a session cookie and dropped from the address bar. `--port 0` (the default) picks a
free port. `Ctrl-C` stops the server. `-p/--profile` applies, as for the TUI (protobuf decoding).

Pages — each one is a URL you can bookmark and reload. A page URL works only in a browser that
already holds the session cookie: sent to a colleague (on the same tunnel), it answers `403`
until they open the token URL themselves — and **handing over the token URL hands over full
read access to the savepoint** for as long as the server runs. The table:

| Page | URL | JSON twin |
|------|-----|-----------|
| Overview: operators, sizes, keyed and operator states | `/` | `/api/operators` |
| Operator: info lines, keyed states, operator states, lookup box | `/op/<id>` | `/api/op/<id>` |
| Every key with all its states, paged | `/op/<id>/keys?cursor=…&limit=…` | `/api/op/<id>/keys?…` |
| One state's entries, paged | `/op/<id>/state?name=<state>&cursor=…` | `/api/op/<id>/state?…` |
| One key: every state's decoded value as a tree, raw bytes in hex | `/op/<id>/key?at=<cursor>` | `/api/op/<id>/key?at=…` |
| Exact lookup by key text | `/op/<id>/lookup?key=<text>` | `/api/op/<id>/lookup?key=…` |
| The rest of a big key: one state's entries, paged | `/op/<id>/entries?at=<cursor>&state=<state>&offset=N` | `/api/op/<id>/entries?…` |
| Search keys (glob, substring, exact), optionally in a key-group range | `/op/<id>/search?q=…&mode=glob\|substring\|exact&from=…&to=…` | `/api/op/<id>/search?…` |
| Exact key and entry counts per state (background pass) | `/op/<id>/count` (`?cancel=1`, `?restart=1`) | `/api/op/<id>/count` |
| Download one state, NDJSON or Parquet | `/op/<id>/download?state=<state>&format=ndjson\|parquet` | — |

The `/api/` rows have exactly the shape of an `--export-state` NDJSON line (`operator`, `subtask`,
`state`, `state_type`, `key`, `key_group`, `value`, and `map_key` / `namespace_hex`); a page's
`next` field is the cursor of the next page (`subtask.keygroup.ordinal`), `null` at the end.

It reads the savepoint **lazily** (one key-group per request, through `reader::StateReader`), so
a large savepoint opens at once and memory stays small. A key page shows at most 200 entries per
state (a MAP state with a million entries, say) and links to the paged entries view for the
rest; the JSON lists such states under `truncated`. Key-groups that cannot be read (a missing or
truncated state file) are skipped with a visible warning naming the subtask, the key-groups and
the file (`unreadable` in the JSON), never silently. At most 64 connections are served at once
(one more gets `503`) and about two requests per CPU read the savepoint at a time (the others
wait), so a burst of large pages cannot exhaust memory.

**Security model.** The server publishes production data, so:

- it binds to **`127.0.0.1` only** by default. `--bind <ip>` overrides that and prints a loud
  warning when the address is not loopback: the traffic is plain HTTP. The printed URL still says
  `127.0.0.1` when bound to `0.0.0.0`/`::`: from another machine, replace it with an address of
  the server;
- every request needs the **session**: the random 128-bit token (from the OS's CSPRNG) is printed
  once and exchanged for an `HttpOnly`, `SameSite=Strict` cookie (by a page that sets it and
  moves on with `<meta http-equiv="refresh">`, so that a token link opened from another site
  keeps the cookie); without it, `403` and no data. Whoever has the token URL has the session:
  treat it like a password;
- **cookies are not port-scoped**: the browser also sends the session cookie (named
  `flink_explorer_<port>`, i.e. the token) to every *other* port on `127.0.0.1`/`localhost`. Any
  other local web server you browse to — or any local user able to run one there — can read it
  and then use this server. On a shared machine, stop the server when you are done;
- with `--open`, the token URL is passed to the browser on its **command line**, which other
  local users can see (`ps`) while that process runs; on a shared machine, copy the URL instead;
- the **`Host` header** must be a loopback name (`localhost`, `127.0.0.1`, `[::1]`) or the bound
  address, which defeats DNS rebinding (a rebinding page sends its own host name);
- only **`GET`/`HEAD`** (`405` otherwise): there is nothing to change;
- every response sends a strict `Content-Security-Policy` (`default-src 'none'`, scripts and
  styles from the server only, no framing), `X-Content-Type-Options: nosniff`,
  `Referrer-Policy: no-referrer`, `Cache-Control: no-store` and same-origin
  `Cross-Origin-Resource-Policy` / `Cross-Origin-Opener-Policy`;
- keys and values are always HTML-escaped (by construction: see `src/web/html.rs`), never
  inlined in a `<script>`; the only files served are the two embedded assets, never a path from
  `_metadata`;
- paths in `_metadata` are not trusted (by the TUI and the export either): a relative state-file
  path that is absolute or contains `..` is refused, and only regular files are opened (a FIFO
  or a device is refused). Absolute `file:` handles are followed, as checkpoints use them.

**Remote savepoints.** Keep the default bind on the remote machine and tunnel to it:

```bash
ssh -L 8080:127.0.0.1:8080 user@flink-host \
    flink-explorer /data/savepoints/savepoint-abc --web --port 8080
# then open the printed URL locally: http://127.0.0.1:8080/?token=…
```

The local port may differ (`ssh -L 9000:127.0.0.1:8080` → open `http://127.0.0.1:9000/?token=…`).

**Search.** The operator page and the keys pages have a search box: **glob** (`*` any run of
characters, `?` one, case ignored — the TUI filter's syntax), **substring** (case ignored, `*` and
`?` literal) or **exact** (the key itself), tested on every key as it is rendered, optionally
within a key-group range (`from`/`to`, inclusive). Each request reads about 4 MB of key-groups
(`budget`, 1 byte to 64 MB) and returns at most 100 matches (`limit`, up to 1000), with the
cursor to resume from and its progress (key-groups and bytes done, from `_metadata`); in a
browser the page keeps going by itself and appends matches as they come, with a **Stop** button,
up to 10 000 matches on the page. Without JavaScript, "Next page of results" does the same one
request at a time. Queries are at most 256 characters. A key the exact lookup cannot locate
(POJO, Kryo, opaque keys) gets a one-click exact scan instead.

**Counts.** "Count keys and entries" (`/op/<id>/count`) starts a background pass over the
operator — one job per operator, kept for as long as the server runs, cancellable
(`?cancel=1`, shown as *cancelling* until the pass has stopped; `?restart=1` counts again). At
most two counts run at once; a third is refused with a message until one ends. Until it is done,
the pages show `≥ N (counting… x %)`; then each state's exact number of keys and of entries
(entries = rows of its download: one per value, per MAP entry, per timer). If some key-groups
could not be read, the numbers stay lower bounds: `≥ N (incomplete: k key-group(s) unreadable)`.

**Downloads.** Each keyed state of the operator page has an NDJSON link (and Parquet, in a build
with `--features parquet`): the same bytes as `--export-state <state> -o -` (and the same Parquet
file), streamed as it is read. An NDJSON download holds one key-group in memory; a Parquet one
also buffers a whole 65 536-row group before writing it (about 17 MB measured on our 1M-key test
savepoint, more with large values). At most two downloads stream at once (one
more gets `503`), which bounds that memory, and a download whose client reads nothing for 45 s
is aborted, freeing its place; a download never takes one of the page-reading slots, so stalled
downloads cannot hold up the pages. A download that cannot be read at all answers an error (the
cause goes to the server's log, not to the browser); one that hits a malformed key-group, or
whose client goes away, is aborted (the browser shows a failed download, never a short file
passing for a whole one).

**Measured** on a 211 MB savepoint (1M keys, 128 key-groups, release build, local disk): a glob or
substring search through all of it, 43 requests, 1.6 s; a key-group range of 2, 0.3 s; the count of
every state, 1.5 s; NDJSON downloads of 155 MB at ~190 MB/s and 412 MB at ~150 MB/s; peak RSS of
the server through all of it, 35 MB.

**Limits.** Keys come in **on-disk order** (key-group by key-group, then as stored), not sorted.
The lookup is exact, on the key as it is rendered; it cannot locate POJO/Kryo keys (their
`hashCode()` is user code), and offers a scan instead.
Operator-state elements are listed (name, mode, element count) but not decoded: use the TUI.
A build without the UI: `cargo build --release --no-default-features`.

## Configuration

### Profile

Profiles are stored next to the binary in `config/profiles/<name>.toml`. Relative `proto_dirs` are
resolved from the binary directory. See [`profile.example.toml`](profile.example.toml).

```
flink-explorer(.exe)
config/
├── config.toml              # last used profile
└── profiles/
    └── asse-verts.toml      # proto dirs + patterns + bindings
protos/                      # .proto files (referenced by proto_dirs)
```

```toml
name = "asse-verts"
proto_dirs = ["./protos"]

[[proto_patterns]]
class_field = "className"
bytes_field = "protoBytes"
resolve = "auto"
```

### Proto Decoding

When a POJO has fields matching a `proto_pattern`, the tool:

1. Reads the `class_field` value (e.g., `"fr.asse.proto.Effectif$FicheJoueur"`).
2. Reads the `bytes_field` raw bytes.
3. Resolves the proto message type (path overrides → explicit `[bindings]` → auto-resolve).
4. Decodes the bytes using the compiled `.proto` descriptors.

| Strategy | Description | Example |
|----------|-------------|---------|
| `auto` | Strip the Java outer class (`$` convention) | `Effectif$FicheJoueur` → `fr.asse.proto.FicheJoueur` |
| `direct` | Use the className as-is | `fr.asse.proto.FicheJoueur` |

## Architecture

```
src/
├── main.rs               # CLI entry (clap)
├── lib.rs                # library root
├── parser/
│   ├── java_deser.rs     # Java DataOutputStream primitives + StringValue
│   ├── metadata.rs       # top-level _metadata binary reader
│   ├── state_handle.rs   # state-handle tag dispatch
│   ├── keyed_state.rs    # key-group block parser + per-state metadata (incl. timers)
│   ├── pojo.rs           # POJO structure parser (recursive, framed snapshots)
│   ├── value.rs          # ★ single source of truth: raw bytes → serde_json::Value
│   └── kafka.rs          # Kafka connector state decoders (source/sink), verified vs. the connector
├── reader/               # StateReader: lazy, per-key-group random access to keyed state
├── operators.rs          # operator metadata + decoded operator state (Kafka, broadcast, lists)
├── proto/
│   ├── compiler.rs       # protox .proto → DescriptorPool
│   ├── decoder.rs        # prost-reflect DynamicMessage decoder
│   └── mod.rs            # ProtoContext: patterns + bindings + resolve
├── profile/storage.rs    # profile TOML persistence
├── sst/reader.rs         # read-only RocksDB/SST reader (external state files)
├── tui/
│   ├── app.rs            # main event loop + render
│   ├── tree.rs           # tree widget: navigation state, pages of keys asked of the reader
│   ├── jobs.rs           # background key count and `E` export (worker threads, cancellable)
│   ├── counts.rs         # key counts cache (user cache dir, counts only)
│   ├── detail.rs         # key/value hex overlay + operator-info overlay
│   ├── hex_view.rs       # hex dump widget
│   ├── filter.rs         # key filter (pattern / exact / key-group range)
│   ├── profile_picker.rs # profile selection overlay
│   └── loading.rs        # loading / progress screen
├── reader/               # lazy StateReader: one key-group per call, exact key lookup
├── web/                  # `--web` (feature `web`, default)
│   ├── mod.rs            # request guards (method, Host, token/cookie, headers)
│   ├── html.rs           # Html newtype: escaping as a type invariant
│   ├── pages.rs          # routes: HTML pages + /api/ JSON twins
│   ├── http.rs           # hyper/tokio glue (the only file that knows them)
│   └── assets/           # app.css, app.js (embedded with include_str!)
├── export.rs             # `E` JSON export, streamed from the reader (same decoder as the TUI)
└── error.rs              # unified error types
```

The decoder in `src/parser/value.rs` is the **single source of truth** for turning a state value's
bytes into a `serde_json::Value`; both the TUI and the JSON export render that Value. To support a
new type, add it there once.

## Test Fixtures (contributors)

The Rust tests read savepoint fixtures that are **generated, not committed** (gitignored). They are
produced by a real Flink 1.20 cluster spun up with Testcontainers from `test-flink-job/`:

```bash
cd test-flink-job && mvn verify    # ~3 min: builds the shaded jar, then generates the fixtures
cd .. && cargo test                # reads tests/fixtures/savepoint-gitlab and savepoint-kafka
```

- `SavepointGeneratorIT` → `tests/fixtures/savepoint-gitlab/` — exercises every state type via
  `ClusterJob`.
- `KafkaSavepointGeneratorIT` → `tests/fixtures/savepoint-kafka/` — a KafkaSource → KafkaSink job
  (with a Testcontainers Kafka broker) that produces continuously so the savepoint captures an
  in-flight transaction.

Tests that need a fixture **skip** cleanly when it is absent. See `CLAUDE.md` for the Testcontainers
gotchas already handled.

## Development

`cargo test; cargo clippy` is the check loop. Dependencies are kept current and **free of
unmaintained crates** — run `cargo audit` and resolve every advisory (including `unmaintained` /
`unsound` warnings) when touching dependencies.

## Binary Format Documentation

The Flink savepoint binary format (verified against the Apache Flink 1.20 Java source) is documented
in:

- **[docs/parsing-format.md](docs/parsing-format.md)** — metadata structure, key-group blocks, entry
  format, StringValue encoding, PojoSerializer, serializer snapshots, and value formats.
- **[docs/magic-numbers.md](docs/magic-numbers.md)** — the magic numbers / header constants, named as
  in Flink.

## License

See LICENSE file.
