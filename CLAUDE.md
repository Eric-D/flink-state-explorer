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

Two ITs generate two fixtures (both gitignored): `SavepointGeneratorIT` → `savepoint-gitlab`
(all state types, via `ClusterJob`); `KafkaSavepointGeneratorIT` → `savepoint-kafka`
(KafkaSource→KafkaSink via `KafkaJob` + a Testcontainers Kafka broker). The Kafka IT is separate so
a broker hiccup can't break the main fixture; records are produced from inside the broker and the
Flink containers reach it via the in-network listener `kafka:19092`. Gotchas handled: the Kafka
sink's default `transaction.timeout.ms` (1h) exceeds the broker's `transaction.max.timeout.ms`
(15min) → set to 600000 in `KafkaJob`; the connector pulls an older `jackson-core` onto the test
classpath → pinned to 2.17.2 in the pom.

Gotchas already handled in the pom/IT (don't re-debug):
- **Docker API version**: docker-java defaults to API v1.32 which Docker ≥25 rejects
  ("client version 1.32 is too old"). Fixed by `-Dapi.version=1.43` in the failsafe
  `<systemPropertyVariables>`. Symptom if missing: instant *"Could not find a valid Docker
  environment"* with no logs.
- The savepoint is written to `/tmp/flink-savepoints` **inside** the container (the `flink` uid
  9999 can't mkdir `/savepoints` at root-owned `/`) and copied out with the Docker API, so host
  files are owned by you (a bind mount breaks `mvn clean`).
- `state.storage.fs.memory-threshold: 20 mb` forces small state inline → a single self-contained
  `_metadata`.

## Decoder architecture — IMPORTANT

**`src/parser/value.rs` is the single source of truth for binary value decoding.** It turns a
state value's raw bytes into a `serde_json::Value`: `decode_state_value(bytes, value_type,
pojo_info, proto)`. Both the JSON export (`src/export.rs`) and (target) the TUI render this Value;
do **not** add a second decoder. To support a new type, add it here once.

- `src/parser/keyed_state.rs` extracts state metadata + the value serializer **type string** per
  state (e.g. `Long`, `Pojo`, `List<String>`, `Map<String,Pojo>`, `Tuple<String,Long>`, `int[]`,
  `Kryo`). The value serializer is the **first** snapshot after the options (not the last — a
  POJO's last *nested field* serializer would mislabel it). POJO info is matched to a state **by
  class name** (`extract_value_pojo_classes`), because `extract_pojo_infos` dedupes by class.
- `src/parser/pojo.rs` parses POJO *structure* (field names/types) from `PojoSerializerSnapshot`.
- Protobuf: a POJO with `className` + `protoBytes` fields is decoded against a `.proto` when a
  profile with a matching `[[proto_patterns]]` is loaded (`src/proto/`). `.proto` files are
  gitignored (project-specific).

### Decoded correctly
Scalars, date/time (`Date`/`Timestamp`/`Instant`; note: Flink serializes `java.time.LocalDate`/
`LocalTime`/`LocalDateTime` POJO fields with **Kryo**, so they show as `[Kryo]` — the dedicated
`Local*Serializer` formats are still handled in `value.rs` for the explicit-state case, layouts
verified vs Flink: `LocalDate`=int+byte+byte, `LocalTime`=byte+byte+byte+int, `LocalDateTime`=13B),
Enum (as name), nested POJO, `byte[]`/`String[]`, primitive arrays
(`int[]/long[]/double[]/…`), Tuples, keyed `ListState` (delimiter-separated), `MapState`
(keys + values, incl. POJO values), protobuf-in-POJO (with a profile), **window state**
(`window-contents`) and **timers** (priority-queue states `_timer_state/...`, shown as the firing
timestamp), **TTL-wrapped values** (`Ttl<T>` → `{ _ttlLastAccess, value }`, via `TtlSerializer`),
and **broadcast maps** (best-effort String key/value). Keyed state is merged across **all** subtasks
(full key set), and the key-group parser handles the shared keyed/priority-queue stateId space
(terminating on `END_OF_KEY_GROUP_MARK`).

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
`[int outerLen][int version][int innerLen][payload]`.

### Operator info
`CachedOperator::info_lines()` aggregates everything extractable about an operator (OperatorID/uuid,
parallelism, max parallelism, subtasks, coordinator-state presence, key type, keyed-state names,
operator-state names + distribution mode, sizes, fully-finished). Shown via the `i` key in the TUI
and in the JSON export (`info` block per operator; the export includes **all** operators, not just
keyed ones). Operator states are surfaced with their real name + mode (`SPLIT_DISTRIBUTE` / `UNION`
/ `BROADCAST`) and decoded element values.

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
  rest of that POJO (length unknown), so following fields show `null`.
- The **window namespace** itself (TimeWindow start/end) isn't surfaced as a field — only the
  window's buffered contents and its firing timer are shown; if one key has multiple windows, only
  the first window's contents is kept per key.
- **Snapshot compression**: the keyed/operator backend headers carry a `usingKeyGroupCompression` /
  `usingStateCompression` boolean (Flink default OFF). We do **not** read it; if a savepoint was
  written with snapshot compression enabled, each key-group is snappy-compressed and inline decoding
  would produce garbage. (Our fixtures are uncompressed.)
- **Broadcast / operator-state element decoding is best-effort String**: broadcast maps decode when
  the key/value are Strings (the common case; `decode_broadcast_map`), and operator `ListState`
  elements decode as `StringValue`; other element types fall back to a byte preview (the operator
  backend's element serializers aren't parsed).
- **Channel state** (`input_channel_state` / `result_subpartition_state`, unaligned checkpoints) is
  parsed but not displayed — it does not appear in canonical savepoints anyway.
- RocksDB incremental / changelog state is not decoded (canonical heap savepoints only).

## Binary format references (verify against apache/flink)

**When implementing/changing any byte-level decoding, verify the format against the Apache Flink
source** (branch `release-1.20`, https://github.com/apache/flink) rather than guessing. Raw files:
`https://raw.githubusercontent.com/apache/flink/release-1.20/<path>`. Key references (verified):

- **Per-key-group layout** — `flink-runtime/.../state/FullSnapshotAsyncWriter.java`: keyed states
  **and** priority-queue states (timers) share **one `short` stateId space** —
  `writeShort(kvStateId())` precedes each state's entries; the group ends with
  `writeShort(END_OF_KEY_GROUP_MARK)`. Each entry is `BytePrimitiveArraySerializer.serialize(key)`
  then `serialize(value)` (each = int length + bytes).
- **End marker** — `flink-runtime/.../state/FullSnapshotUtil.java`: `END_OF_KEY_GROUP_MARK = 0xFFFF`,
  `FIRST_BIT_IN_BYTE_MASK = 0x80`.
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
- **Enum** — `EnumSerializerSnapshot`: `writeUTF(enumClass)` + `int(n)` + `n × writeUTF(constant)`
  (constants in ordinal order).
- **String** — Flink `StringValue`: var-int length = `charCount + 1`, then var-int chars.
- Magic numbers / metadata layout: see `docs/magic-numbers.md`, `docs/parsing-format.md`.

## Tests
- `cargo test` — needs `tests/fixtures/savepoint-gitlab/` (run `mvn verify` first).
- `tests/decode_e2e_test.rs` — protobuf, tuples, primitive arrays, full-POJO, scrollbar.
- `tests/coverage_audit.rs` — dumps the full operator/state inventory + decoded values (diagnostic).
- `tests/gitlab_savepoint_test.rs` — exhaustive per-state assertions.

## Dependencies — IMPORTANT

**No unmaintained crates.** When touching dependencies, run `cargo audit` and resolve **every**
advisory, including `unmaintained`/`unsound` *warnings* (not just vulnerabilities). The tree must
stay clean.
- Prefer **removing** a dep if it's unused (`tui-tree-widget` and `dirs` were dead and were dropped;
  removing `tui-tree-widget` also deduped a second `ratatui`).
- The index cache (`src/index/cache.rs`) uses **`postcard`** (not `bincode`, which is unmaintained —
  RUSTSEC-2025-0141, and whose 3.0.0 is a serde-less tombstone). Use
  `postcard = { default-features = false, features = ["use-std"] }` to avoid `heapless →
  atomic-polyfill` (also unmaintained). An undecodable cache falls back to a full rebuild, so the
  on-disk format may change freely.
- Keep `ratatui`/`crossterm` current (≥0.30/0.29) — older `ratatui` pulls `paste`/`lru` advisories.

<!-- MANUAL ADDITIONS END -->
