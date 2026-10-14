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
- **JSON export** (`E`) — writes `export.json` next to the savepoint with the decoded model (up to
  100 000 keys per operator, flagged `truncated` past that; `--export-state` streams every key).
- **Web UI** (`--web`) — the same savepoint in a browser, read-only, loopback-only, token-guarded;
  see [Web UI](#web-ui).
- **Filtering** — by key prefix, exact match, or key-group range.
- **Profiles** — per-project proto dirs, patterns, and bindings.
- **Index caching** — fast reload with staleness detection (auto-rebuilds if stale).

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

# Skip the index cache (always re-parse; .flink-explorer-cache is neither read nor written)
./target/release/flink-explorer --no-cache /path/to/savepoint-dir
```

`flink-explorer --help` lists the flags: `-p/--profile <name>`, `--no-cache`.

A state file that is missing, unreadable or shorter than `_metadata` says is never taken for
"no state": the operator is flagged `⚠ N state file(s) not read` (details under `i`), the other
subtasks still load, and such a partial index is not cached. `--list-states` and `--export-state`
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
| **Partition** | keyBy value | `rocheteau (7 states)` |
| **State entry** | State name + decoded value | `matchs-joues: 416` |
| **Expanded** | POJO fields / MAP pairs | `nom: "Rocheteau"` |

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `j` / `↓` | Move down |
| `k` / `↑` | Move up |
| `PageDown` / `PageUp` | Move a page (the tree's height) down / up; past a `↓`/`↑` row, the keys or operator-state elements scroll by the rest of the page |
| `l` / `Enter` / `→` | Expand; on a value or an operator-state element, open its hex detail |
| `x` | Hex detail of the selected value (keyed value, MAP blob, operator-state element): its raw bytes |
| `h` / `Esc` / `←` | Collapse / clear filter / close overlay |
| `g` / `Home` | Jump to first |
| `G` / `End` | Jump to the last row (of the keys or elements shown: `↓` past it scrolls) |
| `i` | Operator info (OperatorID, parallelism, state names, …) |
| `E` | Export the decoded model to `export.json` |
| `S` | Savepoint summary |
| `I` | List loaded `.proto` files |
| `/` | Open the filter bar (Tab cycles prefix / exact / key-group) |
| `P` | Profile picker |
| `H` | Toggle hiding empty operators |
| `D` | Toggle debug mode (extra type tags, key-groups) |
| `q` / `Ctrl-C` | Quit (`Ctrl-C` also from the filter bar and overlays, and while loading — then the process is interrupted mid-build and exits with 130; once loaded, quitting is a normal exit, 0) |

In the detail overlay: `Tab` switches the key/value panel, `↑`/`↓` scroll, `Esc` closes. The value
panel holds the bytes as the index stores them — for a MAP state, the per-key blob it assembles
(titled `per-key blob`: `int count`, then per entry `int len` + namespace (`VoidNamespace` byte +
map key), `int len` + value behind its `NullableSerializer` null byte); the key panel holds the key
as rendered (raw key bytes are not kept).

## JSON Export

Press `E` in the TUI to write `export.json` into the savepoint directory. It mirrors the TUI model
and includes **all** operators (not just keyed ones): an `info` block per operator, the
`state_descriptors` (with `value_class` when known), and the per-key decoded values. The export and
the TUI share the same decoder (`src/parser/value.rs`), so they never diverge.

The index behind the TUI and this export keeps at most **100 000 keys per operator**, shared
between its subtasks. Past that, the TUI says so (`100000 of 1000000 keys shown`) and the export
sets `truncated: true` at the root and on the operator, with its `stored_keys` and `total_keys`.
To get every key of a state, use `--export-state`, which streams the savepoint instead.

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

**Limits.** Keys come in **on-disk order** (key-group by key-group, then as stored), not sorted.
Key counts, search by substring and downloads come later (#23). The lookup is exact, on the key
as it is rendered; it cannot locate POJO/Kryo keys (their `hashCode()` is user code) and says so.
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
├── index/
│   ├── builder.rs        # savepoint → structured cache model (merges all subtasks)
│   └── cache.rs          # cached model + postcard persistence (staleness-checked)
├── proto/
│   ├── compiler.rs       # protox .proto → DescriptorPool
│   ├── decoder.rs        # prost-reflect DynamicMessage decoder
│   └── mod.rs            # ProtoContext: patterns + bindings + resolve
├── profile/storage.rs    # profile TOML persistence
├── sst/reader.rs         # read-only RocksDB/SST reader (external state files)
├── tui/
│   ├── app.rs            # main event loop + render
│   ├── tree.rs           # tree widget (Operator/Partition/Entry/MapEntry)
│   ├── detail.rs         # key/value hex overlay + operator-info overlay
│   ├── hex_view.rs       # hex dump widget
│   ├── filter.rs         # key filter (prefix / exact / key-group range)
│   ├── profile_picker.rs # profile selection overlay
│   └── loading.rs        # loading / progress screen
├── reader/               # lazy StateReader: one key-group per call, exact key lookup
├── web/                  # `--web` (feature `web`, default)
│   ├── mod.rs            # request guards (method, Host, token/cookie, headers)
│   ├── html.rs           # Html newtype: escaping as a type invariant
│   ├── pages.rs          # routes: HTML pages + /api/ JSON twins
│   ├── http.rs           # hyper/tokio glue (the only file that knows them)
│   └── assets/           # app.css, app.js (embedded with include_str!)
├── export.rs             # JSON export (same decoder as the TUI)
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
