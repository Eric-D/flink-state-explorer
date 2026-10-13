# Flink Savepoint Test Generator

Mini Flink 1.20 application that generates a canonical savepoint with all state types
supported by `flink-explorer`.

## Generating the savepoint (recommended: Testcontainers)

`SavepointGeneratorIT` spins up a real Flink 1.20 cluster (JobManager + TaskManager) in
Docker via Testcontainers, submits `ClusterJob`, triggers a CANONICAL savepoint, and copies
it to `tests/fixtures/savepoint-gitlab/`. Then the Rust tests read it directly.

```bash
# Requires Docker + JDK 11+ (the test harness itself runs fine on newer JDKs).
cd test-flink-job
mvn verify                       # builds the shaded jar, then generates the savepoint

# Override the output location if needed:
mvn verify -Dsavepoint.output.dir=/abs/path/to/savepoint

# Back in the repo root, the fixture-backed tests now pass:
cd ..
cargo test --test gitlab_savepoint_test
```

Implementation notes:
- The savepoint is written inside the container (to `/tmp/flink-savepoints`, which the
  `flink` user can create) and copied out with the Docker API, so host-side files are owned
  by the current user — a bind mount would leave them owned by uid 9999 and break `mvn clean`.
- The cluster uses JobManager checkpoint storage (`execution.checkpointing.storage:
  jobmanager`, Flink's default when `execution.checkpointing.dir` is not set). It returns every state handle
  inline, so the savepoint is a single self-contained `_metadata` and no shared JM/TM volume is
  needed; each handle is capped at 5 MiB. The inline threshold
  (`execution.checkpointing.data-inline-threshold`, formerly `state.storage.fs.memory-threshold`)
  only applies to filesystem storage, and is at most 1 MiB.
- `-Dapi.version=1.43` is pinned in the failsafe config: docker-java otherwise uses Docker
  API v1.32, which daemons ≥ 25 reject with *"client version 1.32 is too old"*.

## Big fixture for performance work (opt-in)

`BigSavepointGeneratorIT` generates `tests/fixtures/savepoint-big` (gitignored): one keyed
operator (`BigStateJob`, parallelism 2, default max parallelism 128) holding N String keys
`key-0000000..` with a `ValueState<Long>` (`big-counter`), a 55-75 char `ValueState<String>`
(`big-label`) and a 2-3 entry `MapState<String,Long>` (`big-stats`). Every value is a pure
function of the key index (see the `*Of(int)` helpers in `BigStateJob`), so it can be asserted
without a reference dump. It is skipped unless opted in, and starts its own containers, so a
plain `mvn verify` is not slowed down:

```bash
cd test-flink-job
mvn -B -Dbig.savepoint=true -Dit.test=BigSavepointGeneratorIT verify
# optional: -Dbig.savepoint.keys=2000000 -Dbig.savepoint.tm.memory=6g -Dbig.savepoint.output.dir=/abs/path
```

What makes it different from the other fixtures:
- The cluster uses `FileSystemCheckpointStorage` (`execution.checkpointing.storage: filesystem`): the
  other ITs sit on the default `JobManagerCheckpointStorage`, which keeps every handle in memory
  and caps it at 5 MB (*"Size of the state is larger than the maximum permitted memory-backed
  state"*), whatever the inline threshold says. That threshold is then left at its default
  (20 kb), so each subtask's keyed state is written to its **own file next to
  `_metadata`** (`RelativeFileStateHandle`), the layout production savepoints have and the only
  fixture exercising that read path.
- JM and TM share no volume: the JM writes `_metadata` and the TM writes the state files to the
  same `savepoint-<id>` path on their own filesystems; the IT copies both into one flat directory.
- The TaskManager runs with `taskmanager.memory.process.size: 4g` (managed memory shrunk to 10%,
  the hashmap backend keeps state on the heap) so a few million keys fit.
- The savepoint is triggered once the process vertex's `read-records` REST metric reaches N,
  not after a fixed sleep. Budget roughly 210 bytes per key on disk: the default 1,000,000 keys
  give a savepoint of about 200 MB in 2-3 minutes.

## State Types Covered

### Key Types
- `String` key (most common)
- `Long` key
- `PojoKey` (POJO with String, String, Enum fields)

### Value Types (ValueState)
- `Long` — simple counter
- `String` — simple label
- `Boolean` — simple flag
- `Date` (`java.util.Date`) — epoch millis
- `SimpleEvent` POJO — String, long, boolean, Date, Enum fields
- `ComplexRecord` POJO — all date/time types, nested POJO, String[], byte[], Enum

### Map Types (MapState)
- `Map<String, SimpleEvent>` — String key, POJO value
- `Map<Long, Date>` — Long key, Date value
- `Map<EventType, Boolean>` — Enum key, Boolean value
- `Map<String, Date>` — String key, Date value (POJO-keyed operator)

### List Types (ListState)
- `List<String>` — list of strings
- `List<SimpleEvent>` — list of POJOs

### Serialization Patterns Tested
- PojoSerializer flags byte (`0x02` NO_SUBCLASS)
- NullableSerializer wrapper (`0x00` prefix)
- Nested POJO (NestedAddress inside ComplexRecord)
- Enum ordinal resolution
- StringValue encoding (variable-length int)
- CompositeTypeSerializerSnapshot (List, Map)
- Date/Time: Date, LocalDateTime, LocalDate, LocalTime, Instant
- StringArraySerializer (String[])
- BytePrimitiveArraySerializer (byte[])

## Usage

```bash
# Build and run (requires Maven + JDK 11+)
cd test-flink-job
mvn compile exec:java

# The savepoint is created in:
#   target/savepoint-test/savepoint-<checkpointId>-<hash>/

# Explore it:
cd ..
cargo run -- test-flink-job/target/savepoint-test/savepoint-*/ --no-cache
```

## Updating the Test Fixture

After generating a new savepoint, copy it to `tests/fixtures/` to use as an
automated test fixture:

```bash
cp -r test-flink-job/target/savepoint-test/savepoint-*/ tests/fixtures/savepoint-all-types/
```
