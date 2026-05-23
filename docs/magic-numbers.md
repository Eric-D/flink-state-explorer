# Flink Magic Numbers & Constants Reference

> Constant names and values verified against the Apache Flink source, branch `release-1.20`
> (https://github.com/apache/flink), and validated against real savepoints. Names below match
> Flink's own constant names so they can be grep'd in the Flink tree.

## Metadata File

| Value | Type | Flink constant | Source (Flink class) |
|-------|------|----------|---------------------|
| `0x4960672D` | `int32` | `HEADER_MAGIC_NUMBER` | `runtime.checkpoint.Checkpoints` |
| `0xC96B1696` | `int32` | `MASTER_STATE_MAGIC_NUMBER` | `runtime.checkpoint.metadata.MetadataV2V3SerializerBase` |
| 3, 4, 5 | `int32` | Metadata version | `MetadataV2V3Serializer` / `MetadataV4Serializer` / `MetadataV5Serializer` |

## State Handle Tags

### StreamStateHandle (byte tag)

| Tag | Name | Description |
|-----|------|-------------|
| 0 | `SSH_NULL` | No data |
| 1 | `SSH_BYTE_STREAM` | Inline bytes (name + data) |
| 2 | `SSH_FILE` | External file (path + size) |
| 3 | `SSH_KEY_GROUPS` | Nested KeyGroupsStateHandle |
| 6 | `SSH_RELATIVE` | Relative path in savepoint dir |
| 15 | `SSH_SEGMENT` | File segment (path + offset + size) |
| 16 | `SSH_EMPTY_SEGMENT` | Empty segment |

### KeyedStateHandle (byte tag)

| Tag | Name | Description |
|-----|------|-------------|
| 0 | `KSH_NULL` | No keyed state |
| 3 | `KSH_KEY_GROUPS` | KeyGroupsStateHandle |
| 5 | `KSH_INCREMENTAL` | IncrementalRemoteKeyedStateHandle |
| 7 | `KSH_SAVEPOINT_KEY_GROUPS` | Canonical savepoint (SavepointKeyGroupsStateHandle) |
| 8 | `KSH_CHANGELOG` | ChangelogStateHandle |
| 9 | `KSH_CHANGELOG_BYTE` | Changelog byte increment |
| 10 | `KSH_CHANGELOG_FILE` | Changelog file increment |
| 11 | `KSH_INCREMENTAL_V2` | IncrementalRemoteKeyed v2 |
| 12 | `KSH_KEY_GROUPS_V2` | KeyGroupsStateHandle v2 |
| 13 | `KSH_CHANGELOG_FILE_V2` | Changelog file v2 |
| 14 | `KSH_CHANGELOG_V2` | ChangelogStateHandle v2 |

### OperatorStateHandle (int32 flag)

| Value | Description |
|-------|-------------|
| 0 | No state |
| 1 | State present (followed by handle data) |

## Serializer Snapshots

### CompositeTypeSerializerSnapshot

| Value | Type | Flink constant | Source (Flink class) |
|-------|------|------|--------|
| `911108` | `int32` | `MAGIC_NUMBER` | `api.common.typeutils.CompositeTypeSerializerSnapshot` |
| `1333245` | `int32` | `MAGIC_NUMBER` | `api.common.typeutils.NestedSerializersSnapshotDelegate` |

> **Important**: the outer (composite) and the nested-delegate use DIFFERENT magic numbers,
> both named `MAGIC_NUMBER` in their respective Flink classes. Getting this wrong breaks
> List/Map/Array/Tuple snapshot parsing.

### Optional map (POJO field map / subclass maps)

| Value | Type | Flink constant | Source (Flink class) |
|-------|------|------|--------|
| `0x4F2C69A3D70` | `long` | `HEADER` | `util.LinkedOptionalMapSerializer` |

`PojoSerializerSnapshotData` writes its field map and subclass maps via
`LinkedOptionalMapSerializer`, which begins each map with `writeLong(HEADER) + writeInt(size)`
then the entries.

### TypeSerializerSnapshotSerializationProxy

| Value | Type | Description |
|-------|------|-------------|
| 2 | `int32` | Proxy version (written before className + snapshotVersion) |

> The proxy version only appears at the **top level** (in `_metadata`).
> Inside framed blocks (e.g., POJO field snapshots), the format is
> `writeVersionedSnapshot` which has NO proxy version prefix.

## PojoSerializer Runtime

### Flags Byte

Written as the first byte when serializing a POJO value:

| Value | Name | Description |
|-------|------|-------------|
| `0x01` | `IS_NULL` | Value is null (no further data) |
| `0x02` | `NO_SUBCLASS` | Exact class match (common case) |
| `0x04` | `IS_SUBCLASS` | Subclass, followed by `writeUTF(className)` |
| `0x08` | `IS_TAGGED_SUBCLASS` | Tagged subclass, followed by `writeInt(tagId)` |
| `0x00` | *(heuristic)* | NullableSerializer wrapper `isNull=false` before real flags |

> **Heuristic**: `flags=0x00` is not a valid PojoSerializer flag. When encountered,
> it typically indicates a `NullableSerializer` wrapper where `0x00` means "not null"
> and the next byte is the real POJO flags byte (`0x02`).

### Null Boolean Convention

```
writeBoolean(field == null)
  true  (0x01) → field is NULL
  false (0x00) → field HAS VALUE
```

> This is the **same** convention used by `NullableSerializer.serialize()`:
> `writeBoolean(value == null)`.

## Key-Group Block

| Value | Type | Flink constant | Source (Flink class) |
|-------|------|------|--------|
| `0xFFFF` | `short` | `END_OF_KEY_GROUP_MARK` | `runtime.state.FullSnapshotUtil` |
| `0x80` | `int` | `FIRST_BIT_IN_BYTE_MASK` | `runtime.state.FullSnapshotUtil` |

Per `FullSnapshotAsyncWriter`, each key-group writes, for **every** state (keyed states **and**
priority-queue states such as timers, in one shared id space): `writeShort(kvStateId)` then the
state's entries, ending with `writeShort(END_OF_KEY_GROUP_MARK)`. Each entry is
`BytePrimitiveArraySerializer.serialize(key)` then `serialize(value)` (each = int length + bytes).

> Because priority-queue (timer) states share the id space, a keyed state's stateId is **not**
> necessarily its index among keyed states (e.g. a window operator's `window-contents` is stateId 1
> when a timer priority-queue occupies stateId 0).

## Key-Group Prefix Length

Based on `maxParallelism` (= number of key groups):

| Range | Prefix bytes | Encoding |
|-------|-------------|----------|
| ≤ 128 | 1 | `writeByte(keyGroup)` |
| > 128 | 2 | `writeShort(keyGroup)` |

Computed by `CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix()`:
`maxParallelism > (Byte.MAX_VALUE + 1) ? 2 : 1`. `maxParallelism` is capped at 32768, so the prefix
is **always 1 or 2 bytes — never 4**.

## Composite Key Format (per entry)

### ValueState / ReducingState / AggregatingState

```
kgPrefix + keyByKey + VoidNamespace(0x00)
```

### MapState

```
kgPrefix + keyByKey + VoidNamespace(0x00) + mapUserKey
```

Each map entry is a separate entry in the key-group block. The map user key
is serialized after the VoidNamespace byte using the map's key serializer.

### ListState

Key: same as ValueState. The **value** is the list serialized by `ListDelimitedSerializer`:
elements written back-to-back, separated by the delimiter byte `,` (`0x2c`), with **no** length
prefix. (Distinct from `api.common.typeutils.base.ListSerializer`, used for a `List` *field* inside
a POJO, which writes `int(size)` + elements.)

### Priority-queue state (timers)

A timer element is serialized by `streaming.api.operators.TimerSerializer`:
`writeLong(MathUtils.flipSignBit(timestamp))` + key serializer + namespace serializer. The
sign-bit flip makes timers sort correctly as unsigned; flip bit 63 back to recover the timestamp.
Window operators carry timer states named `_timer_state/processing_*` and `_timer_state/event_*`.

### Window namespace

Keyed window state (e.g. `window-contents`) uses a non-void namespace: a `TimeWindow` serialized as
two `long`s (start, end) by `streaming.api.windowing.windows.TimeWindow$Serializer`. Non-window
keyed state uses `VoidNamespace` = a single `0x00` byte.

## Variable-Length Integer (VInt)

Used by Flink's `StringValue` and related encodings:

```
7 bits per byte, high bit = continuation
byte & 0x80 == 0 → last byte
byte & 0x80 != 0 → more bytes follow, value in low 7 bits
```

Shift increases by 7 for each continuation byte. Maximum 5 bytes (28-bit shift limit).
