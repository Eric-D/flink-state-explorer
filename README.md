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
  - scalars (`Int`/`Long`/`Short`/`Float`/`Double`/`Boolean`/`String`), date/time
    (`Date`/`Timestamp`/`Instant`; `java.time.Local*` are Kryo-serialized by Flink → shown as `[Kryo]`),
  - `Enum` (resolved to the constant **name**, not the ordinal),
  - `byte[]`, `String[]`, primitive arrays (`int[]`/`long[]`/`double[]`/…), `Tuple<…>`,
  - **recursive POJOs** (nested POJOs and inner classes) rendered as inline JSON,
  - **keyed `ListState`**, **`MapState`** (keys + values, incl. POJO values),
  - **TTL-wrapped** values (`Ttl<T>`), **broadcast** maps, **window contents**, and **timers**
    (shown as the firing timestamp).
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
- **JSON export** (`E`) — writes `export.json` next to the savepoint with the full decoded model.
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

# Skip the index cache (always re-parse)
./target/release/flink-explorer --no-cache /path/to/savepoint-dir
```

`flink-explorer --help` lists the flags: `-p/--profile <name>`, `--no-cache`.

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
│ q:quit ↑↓:nav Enter:expand Esc:collapse /:filter i:info E:export H:empty │
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
| `l` / `Enter` / `→` | Expand / open detail (hex key+value) |
| `h` / `Esc` / `←` | Collapse / clear filter / close overlay |
| `g` / `Home` | Jump to first |
| `G` / `End` | Jump to last |
| `i` | Operator info (OperatorID, parallelism, state names, …) |
| `E` | Export the decoded model to `export.json` |
| `S` | Savepoint summary |
| `I` | List loaded `.proto` files |
| `/` | Open the filter bar (Tab cycles prefix / exact / key-group) |
| `P` | Profile picker |
| `H` | Toggle hiding empty operators |
| `D` | Toggle debug mode (extra type tags, key-groups) |
| `q` | Quit |

In the detail overlay: `Tab` switches the key/value panel, `↑`/`↓` scroll, `Esc` closes.

## JSON Export

Press `E` in the TUI to write `export.json` into the savepoint directory. It mirrors the TUI model
and includes **all** operators (not just keyed ones): an `info` block per operator, the
`state_descriptors` (with `value_class` when known), and the per-key decoded values. The export and
the TUI share the same decoder (`src/parser/value.rs`), so they never diverge.

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
