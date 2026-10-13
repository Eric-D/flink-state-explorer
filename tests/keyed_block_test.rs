//! Key-group block parsing on hand-built blocks: no fixture needed, so these run in CI.
//! Blocks follow `FullSnapshotAsyncWriter`: `short stateId`, entries
//! `[int keyLen][kgPrefix + key + namespace][int valueLen][value]`, the metadata-follows flag
//! (`0x80` on `key[0]`) on the last entry of each state's run, then `END_OF_KEY_GROUP_MARK`.

mod common;

use common::synth::{self, write_string_value, Kind, Ser, StateDef};
use flink_explorer::parser::keyed_state::{
    count_states_in_header, extract_state_metadata, key_group_block, parse_key_group_block,
    read_keyed_stream_header, BlockDefect, KeyGroupParse, KeyedEntry,
};
use flink_explorer::parser::value::decode_map_entry;
use serde_json::json;

/// One `(key bytes, value bytes)` entry; the key is `kgPrefix + key + namespace`.
type Entry = (Vec<u8>, Vec<u8>);

/// A key-group block as `FullSnapshotAsyncWriter.writeKVStateData` writes it: per run a `short`
/// stateId then its entries, the last one carrying the metadata-follows flag on `key[0]`
/// (`setMetaDataFollowsFlagInKey`); then `END_OF_KEY_GROUP_MARK`.
fn block(runs: &[(u16, Vec<Entry>)]) -> Vec<u8> {
    let mut b = Vec::new();
    for (state_id, entries) in runs {
        b.extend_from_slice(&state_id.to_be_bytes());
        for (i, (k, v)) in entries.iter().enumerate() {
            let mut k = k.clone();
            if i + 1 == entries.len() {
                k[0] |= 0x80;
            }
            b.extend_from_slice(&(k.len() as i32).to_be_bytes());
            b.extend_from_slice(&k);
            b.extend_from_slice(&(v.len() as i32).to_be_bytes());
            b.extend_from_slice(v);
        }
    }
    b.extend_from_slice(&0xFFFFu16.to_be_bytes());
    b
}

/// `CompositeKeySerializationUtils.writeKeyGroup`: 1 byte, or 2 above maxParallelism 128.
fn kg_prefix(kg: u16, prefix_len: usize) -> Vec<u8> {
    match prefix_len {
        1 => vec![kg as u8],
        _ => kg.to_be_bytes().to_vec(),
    }
}

/// The key of a VALUE entry: `kgPrefix + StringValue(key) + VoidNamespace`.
fn value_key(kg: u16, prefix_len: usize, key: &str) -> Vec<u8> {
    let mut k = kg_prefix(kg, prefix_len);
    write_string_value(&mut k, key);
    k.push(0);
    k
}

/// The key of a MAP entry: the VALUE layout followed by `StringValue(userKey)`.
fn map_entry_key(kg: u16, prefix_len: usize, key: &str, user_key: &str) -> Vec<u8> {
    let mut k = value_key(kg, prefix_len, key);
    write_string_value(&mut k, user_key);
    k
}

/// `NullableSerializer(LongSerializer)`: `isNull = false`, then the long.
fn nullable_long(v: i64) -> Vec<u8> {
    let mut b = vec![0u8];
    b.extend_from_slice(&v.to_be_bytes());
    b
}

/// A String key of the ASCII character `c` whose VALUE entry key is exactly `total` bytes.
fn key_of_len(c: char, total: usize, prefix_len: usize) -> String {
    let s = (1..total)
        .rev()
        .map(|n| c.to_string().repeat(n))
        .find(|s| value_key(0, prefix_len, s).len() == total)
        .expect("a key of that length exists");
    assert_eq!(value_key(0, prefix_len, &s).len(), total);
    s
}

/// Parse a whole block, returning what was emitted and how it ended.
fn parse(
    data: &[u8],
    prefix_len: usize,
    types: &[&str],
    only_state: Option<u16>,
) -> (Vec<KeyedEntry>, Result<(), BlockDefect>) {
    let types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    let mut out = Vec::new();
    let res = parse_key_group_block(
        data,
        7,
        &KeyGroupParse {
            num_states: types.len(),
            key_type: "String",
            kg_prefix_len: prefix_len,
            key_pojo: None,
            state_types: &types,
            only_state,
        },
        &mut |e| out.push(e),
    );
    (out, res)
}

#[test]
fn compressed_key_group_decodes_like_an_uncompressed_one() {
    use std::io::Write;

    // A minimal key-group block: stateId 0, then one entry
    // [int keyLen][kgPrefix + StringValue("ab")][int valueLen][value].
    let mut block: Vec<u8> = Vec::new();
    block.extend_from_slice(&0u16.to_be_bytes()); // stateId
    let mut key = Vec::new();
    // 1-byte key-group prefix (maxParallelism <= 128), with FullSnapshotUtil's metadata-follows
    // flag (0x80): this is the last entry of the state's run.
    key.push(0x80u8);
    key.push(3u8); // StringValue var-int length = charCount + 1
    key.extend_from_slice(b"ab");
    key.push(0u8); // VoidNamespace
    block.extend_from_slice(&(key.len() as i32).to_be_bytes());
    block.extend_from_slice(&key);
    let value = 7i64.to_be_bytes();
    block.extend_from_slice(&(value.len() as i32).to_be_bytes());
    block.extend_from_slice(&value);
    block.extend_from_slice(&0xFFFFu16.to_be_bytes()); // END_OF_KEY_GROUP_MARK

    // Snappy *framed*, as SnappyStreamCompressionDecorator writes it.
    let mut framed = Vec::new();
    {
        let mut enc = snap::write::FrameEncoder::new(&mut framed);
        enc.write_all(&block).unwrap();
        enc.flush().unwrap();
    }
    assert_ne!(
        framed, block,
        "the framed stream should differ from the raw block"
    );

    let parse = |data: &[u8]| -> Vec<KeyedEntry> {
        let mut out = Vec::new();
        parse_key_group_block(
            data,
            3,
            &KeyGroupParse {
                num_states: 1,
                key_type: "String",
                kg_prefix_len: 1,
                key_pojo: None,
                state_types: &["VALUE".to_string()],
                only_state: Some(0),
            },
            &mut |e| out.push(e),
        )
        .expect("well-formed block");
        out
    };

    let mut buf = Vec::new();
    let inflated = key_group_block(&framed, true, &mut buf).expect("snappy frame decodes");
    assert_eq!(
        inflated,
        block.as_slice(),
        "decompressed bytes differ from the original block"
    );

    let from_plain = parse(&block);
    let from_compressed = parse(inflated);
    assert_eq!(from_plain.len(), 1, "one entry expected");
    assert_eq!(from_plain[0].key, "ab");
    assert_eq!(from_plain[0].value, value.to_vec());
    assert_eq!(from_compressed.len(), from_plain.len());
    assert_eq!(from_compressed[0].key, from_plain[0].key);
    assert_eq!(from_compressed[0].value, from_plain[0].value);

    // And the header flag that selects this path.
    let mut header = Vec::new();
    header.extend_from_slice(&6i32.to_be_bytes());
    header.push(1u8);
    let h = read_keyed_stream_header(&header).expect("header parses");
    assert_eq!(h.version, 6);
    assert!(h.using_key_group_compression);
}

#[test]
fn a_state_filter_skips_other_states_entries() {
    // Two states in one block; asking for stateId 1 must yield only its entry.
    let mut block: Vec<u8> = Vec::new();
    for (state_id, key_char, val) in [(0u16, b'a', 1i64), (1u16, b'b', 2i64)] {
        block.extend_from_slice(&state_id.to_be_bytes());
        // kg prefix 0 with the metadata-follows flag (each run has one entry, so it is the last),
        // StringValue length = 1 char + 1, the char, VoidNamespace.
        let key = vec![0x80u8, 2u8, key_char, 0u8];
        block.extend_from_slice(&(key.len() as i32).to_be_bytes());
        block.extend_from_slice(&key);
        let vb = val.to_be_bytes();
        block.extend_from_slice(&(vb.len() as i32).to_be_bytes());
        block.extend_from_slice(&vb);
    }
    block.extend_from_slice(&0xFFFFu16.to_be_bytes());

    let types = vec!["VALUE".to_string(), "VALUE".to_string()];
    let mut only_one = Vec::new();
    let ctx = |only_state| KeyGroupParse {
        num_states: 2,
        key_type: "String",
        kg_prefix_len: 1,
        key_pojo: None,
        state_types: &types,
        only_state,
    };
    parse_key_group_block(&block, 0, &ctx(Some(1)), &mut |e| only_one.push(e)).unwrap();
    assert_eq!(
        only_one.len(),
        1,
        "only the requested state should be emitted"
    );
    assert_eq!(only_one[0].state_id, 1);
    assert_eq!(only_one[0].key, "b");
    assert_eq!(only_one[0].value, 2i64.to_be_bytes().to_vec());

    let mut all = Vec::new();
    parse_key_group_block(&block, 0, &ctx(None), &mut |e| all.push(e)).unwrap();
    assert_eq!(all.len(), 2, "without a filter both states are emitted");
}

#[test]
fn keys_over_10_000_bytes_do_not_end_the_run() {
    // Issue #15: a key length over 10,000 used to be taken for "the next stateId follows", which
    // dropped that entry and every later one of the key-group, for every state. Exactly at, just
    // over, and well over that threshold, with a 1-byte prefix and with a 2-byte prefix at the
    // last key-group (32767 = 0x7FFF: the flagged key starts 0xFF).
    for (prefix_len, kg) in [(1usize, 5u16), (2, 32767)] {
        let lens = [10_000usize, 10_001, 12_000];
        let mut run0: Vec<(String, i64)> = Vec::new();
        for (i, &len) in lens.iter().enumerate() {
            run0.push((format!("short-{}", i), i as i64));
            let c = (b'a' + i as u8) as char;
            run0.push((key_of_len(c, len, prefix_len), 100 + i as i64));
        }
        run0.push(("tail".to_string(), 99));
        // The long key is also the *last* of a run, so it carries the flag itself.
        let run1 = vec![
            ("x".to_string(), -1i64),
            (key_of_len('z', 12_000, prefix_len), -2),
        ];
        let entries = |run: &[(String, i64)]| -> Vec<Entry> {
            run.iter()
                .map(|(k, v)| (value_key(kg, prefix_len, k), v.to_be_bytes().to_vec()))
                .collect()
        };
        let data = block(&[(0, entries(&run0)), (1, entries(&run1))]);

        let (got, res) = parse(&data, prefix_len, &["VALUE", "VALUE"], None);
        assert_eq!(res, Ok(()), "prefix {}", prefix_len);
        let expected: Vec<(u16, String, i64)> = run0
            .iter()
            .map(|(k, v)| (0u16, k.clone(), *v))
            .chain(run1.iter().map(|(k, v)| (1u16, k.clone(), *v)))
            .collect();
        let actual: Vec<(u16, String, i64)> = got
            .iter()
            .map(|e| {
                (
                    e.state_id,
                    e.key.clone(),
                    i64::from_be_bytes(e.value.as_slice().try_into().unwrap()),
                )
            })
            .collect();
        assert_eq!(actual, expected, "prefix {}", prefix_len);
        assert!(got.iter().all(|e| e.namespace == [0u8]), "VoidNamespace");

        // The state filter follows the same flags through the long keys it skips.
        let (only1, res) = parse(&data, prefix_len, &["VALUE", "VALUE"], Some(1));
        assert_eq!(res, Ok(()));
        let keys: Vec<&str> = only1.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, [run1[0].0.as_str(), run1[1].0.as_str()]);
    }
}

#[test]
fn a_long_map_user_key_mid_run_keeps_the_rest_of_the_key_group() {
    // A MAP entry's key holds the user map key too, so a long *map key* crosses the old 10,000
    // threshold even with a short keyBy key. It sits mid-run: entries follow it in its own run
    // and in the next state's.
    let long_user_key = "u".repeat(12_000);
    let map_run: Vec<Entry> = [("first", 1i64), (long_user_key.as_str(), 2), ("last", 3)]
        .iter()
        .map(|(uk, v)| (map_entry_key(9, 1, "k", uk), nullable_long(*v)))
        .collect();
    let value_run: Vec<Entry> = vec![(value_key(9, 1, "k"), 42i64.to_be_bytes().to_vec())];
    let data = block(&[(0, map_run), (1, value_run)]);

    let (got, res) = parse(&data, 1, &["MAP", "VALUE"], None);
    assert_eq!(res, Ok(()));
    assert_eq!(got.len(), 4, "three map entries and one value");
    let map: Vec<(String, serde_json::Value)> = got[..3]
        .iter()
        .map(|e| {
            assert_eq!((e.state_id, e.key.as_str()), (0, "k"));
            decode_map_entry(&e.namespace, &e.value, "String", "Long", None, None, 0)
        })
        .collect();
    assert_eq!(
        map,
        vec![
            ("first".to_string(), json!(1)),
            (long_user_key.clone(), json!(2)),
            ("last".to_string(), json!(3)),
        ]
    );
    assert_eq!((got[3].state_id, got[3].key.as_str()), (1, "k"));
    assert_eq!(got[3].value, 42i64.to_be_bytes().to_vec());
}

#[test]
fn a_multi_state_block_parses_every_entry_in_order() {
    // Four runs, one of them a timer queue (shared stateId space), each long enough that the
    // block is far past 64 KiB: a stateId `short` followed by the next key length's high bytes
    // reads as 65,536-ish, a "plausible" length here — only the flag tells them apart.
    let types = ["VALUE", "MAP", "VALUE", "TIMER"];
    let n = 3_000usize;
    let mut runs: Vec<(u16, Vec<Entry>)> = Vec::new();
    runs.push((
        0,
        (0..n)
            .map(|i| {
                let key = format!("key-{:05}", i);
                (value_key(3, 1, &key), (i as i64).to_be_bytes().to_vec())
            })
            .collect(),
    ));
    runs.push((
        1,
        (0..n)
            .map(|i| {
                let key = format!("key-{:05}", i / 2);
                (
                    map_entry_key(3, 1, &key, &format!("m{}", i)),
                    nullable_long(i as i64),
                )
            })
            .collect(),
    ));
    runs.push((
        2,
        (0..n)
            .map(|i| {
                let key = format!("key-{:05}", i);
                let mut v = Vec::new();
                write_string_value(&mut v, &format!("label {}", i));
                (value_key(3, 1, &key), v)
            })
            .collect(),
    ));
    runs.push((
        3,
        (0..n)
            .map(|i| {
                // TimerSerializer: kgPrefix, flipSignBit(timestamp), key, VoidNamespace; no value.
                let mut k = kg_prefix(3, 1);
                k.extend_from_slice(&((1_700_000_000_000i64 + i as i64) ^ i64::MIN).to_be_bytes());
                write_string_value(&mut k, &format!("key-{:05}", i));
                k.push(0);
                (k, Vec::new())
            })
            .collect(),
    ));
    let data = block(&runs);
    assert!(data.len() > 4 * 65_536, "block of {} bytes", data.len());

    let (got, res) = parse(&data, 1, &types, None);
    assert_eq!(res, Ok(()));
    assert_eq!(got.len(), 4 * n, "every entry of every state");
    let ids: Vec<u16> = got.iter().map(|e| e.state_id).collect();
    let expected_ids: Vec<u16> = (0..4u16).flat_map(|s| std::iter::repeat_n(s, n)).collect();
    assert_eq!(ids, expected_ids);
    assert_eq!(got[n - 1].key, format!("key-{:05}", n - 1));
    assert_eq!(got[2 * n].key, "key-00000");
    // A timer's value is its firing timestamp, the sign bit flipped back.
    let last = got.last().unwrap();
    assert_eq!(last.key, format!("key-{:05}", n - 1));
    assert_eq!(
        last.value,
        (1_700_000_000_000i64 + n as i64 - 1).to_be_bytes().to_vec()
    );
}

#[test]
fn malformed_blocks_are_reported_not_cut_short_silently() {
    let types = ["VALUE", "VALUE"];
    let entry = |k: &str, v: i64| (value_key(1, 1, k), v.to_be_bytes().to_vec());
    let good = block(&[
        (0, vec![entry("a", 1), entry("b", 2)]),
        (1, vec![entry("c", 3)]),
    ]);
    let (all, res) = parse(&good, 1, &types, None);
    assert_eq!((all.len(), res), (3, Ok(())));
    let keys = |es: &[KeyedEntry]| es.iter().map(|e| e.key.clone()).collect::<Vec<_>>();

    // Cut inside the last entry's value: the two entries before it are kept, and it is reported.
    let cut = &good[..good.len() - 5];
    let (got, res) = parse(cut, 1, &types, None);
    assert_eq!(keys(&got), ["a", "b"]);
    assert!(
        matches!(res, Err(BlockDefect::Truncated { .. })),
        "{:?}",
        res
    );

    // Every entry is there but the end mark is not.
    let no_end = &good[..good.len() - 2];
    let (got, res) = parse(no_end, 1, &types, None);
    assert_eq!(keys(&got), ["a", "b", "c"]);
    assert_eq!(
        res,
        Err(BlockDefect::Truncated {
            offset: no_end.len()
        })
    );

    // A stateId the header does not register (2 states: 0 and 1).
    let unknown = block(&[(0, vec![entry("a", 1)]), (2, vec![entry("c", 3)])]);
    let (got, res) = parse(&unknown, 1, &types, None);
    assert_eq!(keys(&got), ["a"]);
    assert!(
        matches!(
            res,
            Err(BlockDefect::UnknownStateId {
                state_id: 2,
                num_states: 2,
                ..
            })
        ),
        "{:?}",
        res
    );

    // A negative value length, then a key too short to hold its key-group prefix.
    let mut bad_value = good.clone();
    let vl_at = 2 + 4 + entry("a", 1).0.len();
    bad_value[vl_at..vl_at + 4].copy_from_slice(&(-3i32).to_be_bytes());
    let (got, res) = parse(&bad_value, 1, &types, None);
    assert!(got.is_empty());
    assert_eq!(
        res,
        Err(BlockDefect::BadLength {
            offset: vl_at,
            len: -3
        })
    );

    let mut bad_key = good.clone();
    bad_key[2..6].copy_from_slice(&0i32.to_be_bytes());
    let (_, res) = parse(&bad_key, 1, &types, None);
    assert_eq!(res, Err(BlockDefect::BadLength { offset: 2, len: 0 }));

    // A value length running past the block is a truncation, too.
    let mut overlong = good.clone();
    overlong[vl_at..vl_at + 4].copy_from_slice(&0x7fff_fff0i32.to_be_bytes());
    let (_, res) = parse(&overlong, 1, &types, None);
    assert_eq!(res, Err(BlockDefect::Truncated { offset: 2 }));
}

#[test]
fn the_state_id_bound_is_the_count_the_header_declares() {
    // `KeyedBackendSerializationProxy.write` puts the state count (a `short`) before the first
    // StateMetaInfoSnapshot. The heuristics see fewer states here: "UPPER" is not recognised as
    // a state name, and with the namespace snapshots renamed (as for a window's TimeWindow
    // namespace) there is no VoidNamespace to count either. The declared 3 must still bound the
    // stateIds, or the third state's entries would be reported as an unknown stateId.
    let states = [
        StateDef::new("first", Kind::Value(Ser::Long)),
        StateDef::new("UPPER", Kind::Value(Ser::Long)),
        StateDef::new("third", Kind::Value(Ser::Long)),
    ];
    let mut header = synth::header_bytes(Ser::String, &states, false);
    let void = b"VoidNamespaceSerializerSnapshot";
    for i in 0..header.len() - void.len() {
        if &header[i..i + void.len()] == void {
            header[i] = b'X';
        }
    }
    let metas = extract_state_metadata(&header);
    let names: Vec<&str> = metas.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["first", "third"], "the heuristics miss UPPER");
    assert_eq!(count_states_in_header(&header, &metas), 3);
}
