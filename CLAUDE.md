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
`cargo run --release --example bench_index -- tests/fixtures/savepoint-big [build|load|scroll]`
(the TUI load path, headless, with timings and RSS).

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

`src/export_state.rs` deliberately bypasses `src/index`, because the index holds every key and
every raw value in memory. Instead it reads only each subtask's **header** (it ends where the first
key-group begins) to resolve the state's id/type/serializers, then seeks per key-group via
`KeyGroupsStateHandle.offsets`, reading **one key-group at a time** into a reused buffer. Entries of
other states are skipped by their length prefixes without decoding keys or copying values
(`KeyGroupParse::only_state`). Peak memory is one key-group, not one savepoint —
`ExportStats::max_block_bytes` reports it.

A key-group that cannot be read to its end mark (truncated, a bad length, a stateId beyond the
header's state count) is **reported, not silently cut short**: the rows read before the defect are
exported, and `ExportStats::malformed_key_groups` / `first_malformed` count and name it — the CLI
prints a `Warning:` on stderr (even with `-o -`), the index carries the same count in
`CachedKeyedState` and the `i` panel shows it (`Malformed key-groups`).

Two things to know about the rows:
- a **MAP** state exports **exploded**, one row per entry, with `map_key` decoded from the
  namespace (`VoidNamespace + map key`) and the value unwrapped from `NullableSerializer`. The raw
  per-entry bytes are *not* the blob `index::builder` assembles per key, so `decode_map_entry` in
  `parser::value` is shared by both paths;
- `key_group` is the **absolute** key-group, on this path and on the index/TUI path alike. Both
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
- **Index/TUI**: a subtask that cannot be read is recorded in `CachedOperator.load_errors`, and the
  other subtasks still load. The operator gets a red `⚠ N state file(s) not read` label, is never
  hidden by `hide_empty`, and has a `Load errors` line under `i` (and a `load_errors` array in the
  `E` JSON export). The index does not read segment handles: it reports them as a load error.
- **Cache**: a build with load errors is never cached (`may_save_cache`), `--no-cache` neither
  reads nor writes it, and `tui::app::load_index` is where that is decided.
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
`StateReader::open(dir)` parses `_metadata` and nothing else; `new(&savepoint, dir)` borrows one
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
  unsupported handles);
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
  `next` at the following key).

**Errors** are `reader::ReaderError` (`Display`, `std::error::Error`, `From<ReaderError> for
String` so `?` into `String` still works): `Metadata`, `UnknownOperator`/`NoKeyedState`/`NotFound`
(404), `AmbiguousOperator`/`BadCursor` (400), `Unreadable{subtask, first_key_group,
last_key_group, what, error}`/`Malformed{..}` (500). `Cursor::from_str` still errs with a `String`.

**String keys are rendered injectively** (`parser::value::key_string`, used by `decode_key` for a
`String` key or tuple field): a Java string may hold a lone surrogate, which used to become U+FFFD
and merge distinct keys (and make `get` hash the wrong text). Such a string, or one containing a
literal `\u`, renders escaped — lone surrogate `\uXXXX`, backslash `\\` — and every other string as
itself; escaped texts always contain `\u` and plain ones never do. `parse_key_string` inverts it
(`JavaKey::Utf16` holds the units). Values keep their rendering (U+FFFD). `CACHE_FORMAT` 8.

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

## TUI: key cap, text, terminal

- **Key cap.** The index (`src/index/builder.rs`) stores at most `MAX_STORED_PARTITIONS` (100 000)
  keys per operator; `CachedKeyedState::total_keys` still counts every key (`stored_keys()`,
  `is_truncated()`). The kept keys are shared between subtasks by `spread_quotas` (equal shares,
  max-min fair: a subtask with fewer keys than its share keeps them all), re-applied after each
  subtask, so the kept partitions never exceed ~2 × cap between subtasks (each subtask's full key
  map is still built first — #24 removes that); each subtask gives its lexicographically smallest
  keys, and the kept set is sorted once, so paging reads in key order. It used to fill the cap from subtask 0, so past the cap the other subtasks' keys
  were unreachable. `build_index_with_cap` takes the cap as an argument (tests use a small one).
  The structural fix (no cap at all) is #24.
- **Paging** is bounded by the **stored** partitions (`KeyedSnapshot::visible_count`): bounding it
  by the savepoint's key count indexed past the end and panicked. Every count shown makes the cap
  explicit: operator row `100000 of 1000000 keys shown` (`N match in S of T keys shown` under a
  filter, which only searches stored keys), the pager rows, a trailing `… N more keys not indexed`
  row, `i` info (`Keys`), the `S` summary, and the `E` export (`truncated` at the root and per
  operator, plus `stored_keys` / `total_keys`). `--export-state` streams every key regardless.
  Rows are told apart by their `NodeKind` (`Data` / `PagerUp` / `PagerDown` / `Marker`), never by
  their label: a key, a value or an operator-state element may contain `↓` or `…`.
- **Operator-state elements** page like keyed partitions: 500 rows at a time behind `↑`/`↓` pager
  rows (`StateSnapshot::entry_offset`), each row showing the element's **real** index, also under a
  filter (`(12/1200 entries)` in the header). `↓`/`↑` onto a pager row scroll its window by one;
  `PageDown`/`PageUp` move a page (the list's height at the last render) and, at a pager row,
  scroll by the rest of the page (`TreeView::scroll` takes the amount); `G` lands on the last
  `Data` row, never on a pager row.
- **Decoded values are cached per rebuild** (`TreeView::decoded`, by operator × partition ×
  descriptor): `rebuild_flat_list` runs on every expand / collapse / toggle / scroll, and used to
  decode every value of every expanded key each time (~70 ms with a 300k-entry MAP). It keeps only
  what the last rebuild showed (a collapsed key's values are dropped) and is cleared by
  `set_proto_context`. `decode_calls()` counts decodes, for tests.
- **Raw bytes.** `x` (or Enter on a leaf) opens the hex `DetailOverlay` on the selected row's bytes
  (`TreeView::selected_raw_bytes(&index)`): a keyed value — from its state row or any row under
  it — as the index stores it (for a MAP, the per-key blob `index::builder` assembles: the panel
  title says `per-key blob`, its layout is in the README; the key panel holds the rendered key,
  since the index keeps no raw key bytes), or an operator-state element's own bytes, read from
  the `IndexCache` the `App` holds (the tree keeps no copy). `expand()` returns `false` on a leaf,
  and the app then opens the bytes.
- **Text.** Shorten display text only through `src/tui/text.rs` (`width`, `truncate_to_width`,
  `ellipsize`): it measures terminal columns and cuts on a `char` boundary. `&s[..n]` on display
  text panicked as soon as `n` fell inside `é`, CJK or an emoji. Widths come from ratatui's own
  `Span::width` (unicode-width), so no extra dependency.
- **Terminal.** `tui::terminal::TerminalGuard` installs a panic hook **before** raw mode is
  enabled (restore the terminal, then chain to the previous hook, so the message prints on the
  normal screen), and its `Drop` restores raw mode, the alternate screen and the cursor on every
  other way out. Ctrl-C (`app::is_interrupt`) quits from any mode: raw mode delivers it as a key
  event, so `App::handle_key` takes the whole `KeyEvent`. While the index loads, `main.rs` checks
  for it at each progress step (exit 130; the build is not cancellable), and keys typed meanwhile
  are dropped instead of replayed.

## Decoder architecture — IMPORTANT

**`src/parser/value.rs` is the single source of truth for binary value decoding.** It turns a
state value's raw bytes into a `serde_json::Value`: `decode_state_value(bytes, value_type,
pojo_info, proto)`. The JSON export (`src/export.rs`), the streaming export and the TUI
(`src/tui/tree.rs`) all render this Value; do **not** add a second decoder. To support a new type,
add it here once.

- **TUI.** Every label is `value_to_display` of the decoded Value; a MAP's rows are the decoded
  object's entries (`decode_state_value` reads the index's per-key MAP blob via `decode_map_blob`),
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
- **Keys** are decoded in `value.rs` too (`decode_key`), for the index/TUI and the streaming export
  alike. The index groups entries by the rendered key, so the rendering is **injective**:
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
(full key count; the index *stores* at most 100 000 keys per operator, see "TUI: key cap, text,
terminal"), and the key-group parser handles the shared keyed/priority-queue stateId space
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
`CachedOperator::info_lines()` aggregates everything extractable about an operator (OperatorID/uuid,
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
**any** state (binary search over all offsets). The decoded label is the entry's `key`, and the
element's bytes are kept (`ValueRef::Inline`, cache format 8) for the TUI's hex view (`x`), **once**:
in the index (and its cache file); `TreeView` copies only the labels and reads the bytes from the
index when `x` asks (`selected_raw_bytes(&index)`). Their total is the elements' size, which is the
stream's size without compression but **more** with it (each element is kept inflated), and a
label does not bound them: an undecoded element's label is `[N bytes] <16-byte hex preview>`
whatever N. Only an element that could not be read is `ValueRef::Decoded` (label, no bytes).

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
  window's buffered contents and its firing timer are shown; if one key has multiple windows, only
  the first window's contents is kept per key.
- **Snapshot compression** (keyed state) is handled on **both** paths: `extract_keyed_entries`
  reads `usingKeyGroupCompression` from the header — which is never compressed — and inflates each
  key-group through `key_group_block`, so the index/TUI and the streaming export both work on a
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
  reader and the key-group assignment), so the fixture-dependent ones
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
  and a rebuild decodes only values it did not show before. Helpers in `tests/common/tui.rs`.
  `synth::Ser::Ttl(&inner)` writes a `TtlSerializerSnapshot` (composite, outer version 2, nested
  `[LongSerializer, inner]`) and `Val::Ttl(ts, v)` a `TtlValue` (`writeLong(ts)` + the value), per
  `TtlStateFactory.java` (release-1.20).
- `tests/tui_gitlab_parity_test.rs` — the same over every (key, state) of `savepoint-gitlab`
  (2262 pairs, 66 states incl. timers and windows).
- `tests/tui_raw_bytes_test.rs` (CI) — `x` on a keyed value gives its bytes (and the overlay shows
  them), on a MAP the per-key blob; an operator state of 1,200 elements pages to `[1199]` (with its
  bytes) and keeps real indices under a filter; keys and elements whose labels look like pager
  rows (`↓ 1-30 / 99`) do not trap the cursor; `PageDown`/`PageUp` cross 1,200 elements (and 99
  keys) a page at a time; `G` lands on a data row; element bytes are read from the index; the MAP
  overlay title fits 80 columns.
- `tests/decode_e2e_test.rs` — protobuf, tuples, primitive arrays, full-POJO, scrollbar, and
  `AllFieldsPojo` field by field (chars decoded, then explicit markers — no `null`) on the index,
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
  distinct on the export and the index, with their values, Flink's key-group, and the map key in
  `map_key`; BigInteger/BigDecimal/Char layouts; JDK `hashCode()` known answers.
- `tests/export_parquet_test.rs` — **needs `--features parquet`**: the file is well-formed, the
  `value` column is typed per state, map rows keep `map_key`, key-groups are absolute, columns are
  zstd, and the rows read back equal the NDJSON ones. Verified by reading with arrow-rs, not with
  a third-party engine (DuckDB/Polars were not available here).
- `tests/compressed_savepoint_test.rs` — needs `savepoint-compressed`: the fixture really sets
  the compression flag, the index path inflates its key-groups (same key sets as the uncompressed
  run), the streaming export agrees with the index path on those bytes, and the operator states
  (`"user-bob"`, the broadcast map) decode like the uncompressed run's, in the index and the JSON
  export.
- `tests/operator_state_test.rs` — fixture-free (CI): operator-state streams decode the same
  inline, relative / absolute file, segment, and compressed (incl. an element over 64 KiB, i.e.
  several `0x00` Snappy chunks); a missing or truncated stream file shows `[error: …]` entries;
  1M elements index in well under a second (release; 200k in debug) and time grows linearly.
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
  real one, file-backed/inline/compressed parity, every subtask on the index, long keys and long map
  keys on both paths (maxParallelism 32768), a > 50 KB header (321 states) on `list_states`, the
  index and the export, the index reading its header from the header only (a state value that
  spells a state record is not a state), one-letter state names located at their own record, and
  a corrupted key-group reported by the export, the index and the CLI.
- `tests/state_file_errors_test.rs` — fixture-free (CI): a 2-subtask file-backed synthetic
  savepoint with one state file deleted, truncated or (unix) `chmod 000`. The index records an
  error naming the file and keeps the other subtask's keys; the TUI keeps and flags the operator;
  `export_state_ndjson`, `list_states`, `find_state` and the CLI (exit code) fail naming the file;
  no cache is written after a partial build or with `--no-cache`, which does not read it either.
- `tests/cache_test.rs` — round trip, staleness decided from the header without decoding the body,
  header-less (older) caches rejected, `may_save_cache`.
- `tests/tui_cap_test.rs` (fixture-free, CI) — the key cap: a `TestBackend` pages a hand-built
  capped `IndexCache` to the end and back without panicking, with both counts on the operator,
  pager, filter and trailing rows; on synthetic multi-subtask savepoints the kept keys come from
  every subtask in equal shares (each subtask's smallest), a small subtask keeps all its keys, and
  the `E` export carries `truncated` / `stored_keys` / `total_keys`. Also: a key containing `↓` is
  not taken for the pager.
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
- The index cache carries a `cache_format` version (`src/index/cache.rs`): bump it whenever an
  older cache would decode cleanly but hold values this build computes differently — a mismatch, or
  a cache predating the field, falls back to a full rebuild. The file starts with a fixed 24-byte
  header (magic `FXIC`, format, `_metadata` size and mtime), so a stale cache is rejected without
  decoding its body.
- The index cache (`src/index/cache.rs`) uses **`postcard`** (not `bincode`, which is unmaintained —
  RUSTSEC-2025-0141, and whose 3.0.0 is a serde-less tombstone). Use
  `postcard = { default-features = false, features = ["use-std"] }` to avoid `heapless →
  atomic-polyfill` (also unmaintained). An undecodable cache falls back to a full rebuild, so the
  on-disk format may change freely.
- Keep `ratatui`/`crossterm` current (≥0.30/0.29) — older `ratatui` pulls `paste`/`lru` advisories.

<!-- MANUAL ADDITIONS END -->
