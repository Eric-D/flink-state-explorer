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
- `state.storage.fs.memory-threshold` is raised so the small test state is inlined into a
  single self-contained `_metadata`; no shared JM/TM volume is needed.
- `-Dapi.version=1.43` is pinned in the failsafe config: docker-java otherwise uses Docker
  API v1.32, which daemons ≥ 25 reject with *"client version 1.32 is too old"*.

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
