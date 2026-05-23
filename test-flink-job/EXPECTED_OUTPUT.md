# Expected Output — Savepoint Test Validation

Ce document décrit les valeurs attendues pour chaque état du savepoint généré.
Utilisez-le pour valider que `flink-explorer` lit correctement tous les cas.

## Validation rapide

```bash
cargo run -- test-flink-job/target/savepoint-test/savepoint-*/ --no-cache
```

## Operator 1: All Types Processor (String key)

**Clé**: String (ex: `user-alice`, `user-bob`, ...)
**uid**: `all-types-string-key`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `counter` | VALUE Long | Nombre entier (1, 2, ...) | [ ] |
| `label` | VALUE String | `"label-for-user-alice"` | [ ] |
| `active` | VALUE Boolean | `true` ou `false` | [ ] |
| `last-seen` | VALUE Date | Date ISO (ex: `2026-04-03T...Z`) | [ ] |
| `simple-event` | VALUE Pojo | `{ active: true, createdAt: 2026-..., eventType: CREATE, id: "user-alice-evt-0", timestamp: 17... }` | [ ] |
| `complex-record` | VALUE Pojo | Champs: name, count, score, verified, legacyDate, localDateTime (`2024-03-15T14:30:00`), localDate (`2024-06-21`), localTime (`09:15:30`), instant, address (nested `{ city: "TestCity", street: "123 Main St", zipCode: 75001 }`), tags (`["alpha", "beta", "gamma"]`), payload (`"Hello"` ou `[5 bytes]`), status (UPDATE) | [ ] |
| `events-by-type` | MAP\<String,Pojo\> | 4 entrées: clés `CREATE`, `UPDATE`, `DELETE`, `ARCHIVE` → SimpleEvent | [ ] |
| `timestamp-index` | MAP\<Long,Date\> | 2 entrées: timestamps → dates ISO | [ ] |
| `flags-by-category` | MAP\<Enum,Boolean\> | 4 entrées: `CREATE:true`, `UPDATE:false`, `DELETE:true`, `ARCHIVE:false` | [ ] |
| `log-entries` | LIST\<String\> | `["Entry at ...", "Key=user-alice count=0"]` | [ ] |
| `event-history` | LIST\<Pojo\> | Liste de SimpleEvent | [ ] |

## Operator 2: POJO Key Processor

**Clé**: PojoKey `{ region: "EU", entityId: "entity-001", category: CREATE }`
**uid**: `pojo-key-processor`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `latest-event` | VALUE Pojo | SimpleEvent avec id=entityId | [ ] |
| `date-index` | MAP\<String,Date\> | 2 entrées: `first-seen` (30j avant), `last-seen` (maintenant) | [ ] |

**Vérifier**: la clé POJO s'affiche comme `{ category: CREATE, entityId: "entity-001", region: "EU" }`

## Operator 3: Long Key Counter

**Clé**: Long (hash % 100)
**uid**: `long-key-counter`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `element-count` | VALUE Long | Nombre entier | [ ] |

**Vérifier**: la clé s'affiche comme un nombre (ex: `42`)

## Operator 4: Broadcast Join Processor

**Clé**: String
**uid**: `broadcast-join`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `broadcast-last-event` | VALUE Pojo | SimpleEvent | [ ] |
| `broadcast-event-dates` | MAP\<String,Date\> | 1+ entrées | [ ] |
| *(BroadcastState)* | MAP\<String,String\> | `config-a:CONFIG-A`, `config-b:CONFIG-B` (operator state) | [ ] |

**Vérifier**: le BroadcastState apparaît comme un managed-operator state (internal)

## Operator 5: Timer Processor

**Clé**: String
**uid**: `timer-processor`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `last-timer-ts` | VALUE Long | Timestamp futur (now + 10s) | [ ] |
| *(timer state)* | internal | Timers enregistrés visibles en internal | [ ] |

**Vérifier**: les timers sont visibles dans les states internal

## Operator 6: Tuple Reduce Processor

**Clé**: String
**uid**: `tuple-reduce`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `running-sum` | REDUCING Long | Somme (1, 2, ...) | [ ] |
| `tuple2-state` | VALUE Tuple2\<String,Long\> | `("user-alice", timestamp)` | [ ] |
| `tuple3-state` | VALUE Tuple3\<String,Int,Bool\> | `("user-alice", 10, true)` | [ ] |

**Vérifier**: les Tuples s'affichent avec leurs champs (f0, f1, f2)

## Operator 7: Window Counter

**Clé**: String
**uid**: `window-counter`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| *(window state)* | internal | Window aggregation data | [ ] |

**Vérifier**: le namespace est TimeWindow (pas VoidNamespace)

## Operator 8: All Fields POJO Processor

**Clé**: String
**uid**: `all-fields-pojo`

Cet opérateur teste TOUS les types de champs possibles dans un seul POJO.

| State | Type | Description |
|-------|------|-------------|
| `all-fields-pojo` | VALUE AllFieldsPojo | POJO exhaustif |
| `all-fields-map` | MAP\<String,AllFieldsPojo\> | Map avec le même POJO |

### Champs attendus dans AllFieldsPojo

| Champ | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `boolField` | boolean | `true` | [ ] |
| `byteField` | byte | `42` | [ ] |
| `shortField` | short | `1234` | [ ] |
| `intField` | int | `999999` | [ ] |
| `longField` | long | `123456789012` | [ ] |
| `floatField` | float | `3.14` | [ ] |
| `doubleField` | double | `2.718281828` | [ ] |
| `boolBoxed` | Boolean | `false` | [ ] |
| `byteBoxed` | Byte | `-1` | [ ] |
| `shortBoxed` | Short | `null` (intentionnel) | [ ] |
| `intBoxed` | Integer | `42` | [ ] |
| `longBoxed` | Long | timestamp | [ ] |
| `floatBoxed` | Float | `null` (intentionnel) | [ ] |
| `doubleBoxed` | Double | `99.99` | [ ] |
| `stringField` | String | `"hello-user-alice"` | [ ] |
| `nullString` | String | `null` (intentionnel) | [ ] |
| `enumField` | Enum | `UPDATE` | [ ] |
| `dateField` | Date | ISO date | [ ] |
| `localDateTimeField` | LocalDateTime | `2024-03-15T14:30:45.123` | [ ] |
| `localDateField` | LocalDate | `2024-06-21` | [ ] |
| `localTimeField` | LocalTime | `09:15:30.500` | [ ] |
| `instantField` | Instant | ISO date | [ ] |
| `byteArray` | byte[] | `"Hello"` ou `[5 bytes]` | [ ] |
| `intArray` | int[] | `[10, 20, 30, 40, 50]` | [ ] |
| `longArray` | long[] | `[100, 200, timestamp]` | [ ] |
| `doubleArray` | double[] | `[1.1, 2.2, 3.3]` | [ ] |
| `floatArray` | float[] | `[0.1, 0.2, 0.3]` | [ ] |
| `boolArray` | boolean[] | `[true, false, true, true]` | [ ] |
| `shortArray` | short[] | `[1, 2, 3]` | [ ] |
| `stringArray` | String[] | `["alpha", "beta", "gamma", "delta"]` | [ ] |
| `stringList` | List\<String\> | `["list-a", "list-b", "list-c"]` | [ ] |
| `intList` | List\<Integer\> | `[1, 2, 3, 4, 5]` | [ ] |
| `pojoList` | List\<SimpleEvent\> | 2 SimpleEvents | [ ] |
| `stringMap` | Map\<String,String\> | `color:blue, size:large` | [ ] |
| `stringIntMap` | Map\<String,Integer\> | `score:95, level:3` | [ ] |
| `intStringMap` | Map\<Integer,String\> | `1:first, 2:second` | [ ] |
| `nestedPojo` | NestedAddress | `{ city: "Paris", street: "123 Main St", zipCode: 75001 }` | [ ] |
| `nullNested` | NestedAddress | `null` (intentionnel) | [ ] |
| `deepNested` | DeepNested | `{ innerAddress: { city: "Lyon", ... }, innerEnum: ARCHIVE, innerList: ["deep-a", "deep-b"], label: "deep-label" }` | [ ] |

## Operator 9: Extra Types Processor

**Clé**: String
**uid**: `extra-types`

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `short-val` | VALUE Short | Petit entier (0-99) | [ ] |
| `byte-val` | VALUE Byte | Octet (-128 à 127) | [ ] |
| `float-val` | VALUE Float | `3.14 * len` (ex: `31.4`) | [ ] |
| `double-val` | VALUE Double | `2.718 * hash` | [ ] |
| `int-array` | VALUE int[] | `[1, 2, 3, N]` | [ ] |
| `long-array` | VALUE long[] | `[100, 200, timestamp]` | [ ] |
| `simple-string-map` | MAP\<String,String\> | `key-a:value-alpha`, `key-b:value-beta`, `key-xxx:XXX` | [ ] |
| `running-average` | AGGREGATING Double | Moyenne (double) | [ ] |
| `nullable-never-set` | VALUE String | `null` (jamais mis à jour) | [ ] |

## Operator 10: Operator State Sink

**uid**: `operator-state-sink`
**Type**: Non-keyed (CheckpointedFunction)

| State | Type | Valeur attendue | Vérification |
|-------|------|----------------|--------------|
| `operator-buffer` | ListState\<String\> | Derniers éléments traités (max 20) | [ ] |

**Vérifier**: apparaît comme managed-operator state, pas keyed state

---

## Types non couverts (intentionnellement)

| Type | Raison |
|------|--------|
| Kryo | Sérialisation opaque, pas de schema découvrable |
| Avro (SpecificRecord) | Dépendance Avro, format distinct |
| Row / RowData | Table API, schema dynamique |
| Either\<L,R\> | Rarement utilisé en DataStream API |
| RocksDB incremental | Format SST, pas canonical |
