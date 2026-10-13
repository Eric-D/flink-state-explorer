//! Key-group block parsing on hand-built blocks: no fixture needed, so these run in CI.
//! Blocks follow `FullSnapshotAsyncWriter`: `short stateId`, entries
//! `[int keyLen][kgPrefix + key + namespace][int valueLen][value]`, the metadata-follows flag
//! (`0x80` on `key[0]`) on the last entry of each state's run, then `END_OF_KEY_GROUP_MARK`.

use flink_explorer::parser::keyed_state::{
    key_group_block, parse_key_group_block, read_keyed_stream_header, KeyGroupParse, KeyedEntry,
};

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
        );
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
    parse_key_group_block(&block, 0, &ctx(Some(1)), &mut |e| only_one.push(e));
    assert_eq!(
        only_one.len(),
        1,
        "only the requested state should be emitted"
    );
    assert_eq!(only_one[0].state_id, 1);
    assert_eq!(only_one[0].key, "b");
    assert_eq!(only_one[0].value, 2i64.to_be_bytes().to_vec());

    let mut all = Vec::new();
    parse_key_group_block(&block, 0, &ctx(None), &mut |e| all.push(e));
    assert_eq!(all.len(), 2, "without a filter both states are emitted");
}
