//! Fixture-free tests of `parser::value` on hand-written Flink bytes (#25): `char`/`char[]`,
//! strings outside the BMP, null strings, and the explicit markers that replace `null` for what
//! could not be decoded. Each byte layout is the one written by the Flink 1.20 serializer named in
//! the test (flink-core, release-1.20).

use flink_explorer::parser::java_deser::JavaReader;
use flink_explorer::parser::pojo::{
    deserialize_pojo_to_json, read_field_value_pub, PojoField, PojoInfo,
};
use flink_explorer::parser::value::{
    decode_map_entry, decode_pojo, decode_state_value, is_marker, value_to_display,
};
use serde_json::{json, Value};

fn pojo(name: &str, fields: &[(&str, &str)]) -> PojoInfo {
    PojoInfo {
        class_name: format!("com.example.{}", name),
        short_name: name.to_string(),
        fields: fields
            .iter()
            .map(|(n, t)| PojoField {
                name: n.to_string(),
                type_name: t.to_string(),
                nested_pojo: None,
            })
            .collect(),
    }
}

fn decode(bytes: &[u8], value_type: &str) -> Value {
    decode_state_value(bytes, value_type, None, None)
}

/// `PojoSerializer` writes, for a non-null field, `writeBoolean(false)` then the field.
fn field(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![0x00];
    out.extend_from_slice(bytes);
    out
}

/// `PojoSerializer` flags byte `NO_SUBCLASS` (PojoSerializer.java:51), then the fields.
fn pojo_bytes(fields: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![0x02];
    for f in fields {
        out.extend_from_slice(f);
    }
    out
}

const INT_7: [u8; 4] = [0, 0, 0, 7];

#[test]
fn char_value() {
    // CharSerializer.serialize = writeChar: 2 bytes, high byte first.
    assert_eq!(decode(&[0x00, 0x41], "Char"), json!("A"));
    assert_eq!(decode(&[0x00, 0xE9], "Char"), json!("é"));
    assert_eq!(decode(&[0x20, 0xAC], "Char"), json!("€"));
    // A lone surrogate is a valid Java char but no Rust char: its Java escape, so distinct
    // chars stay distinct (U+FFFD would merge them, e.g. as map or tuple keys).
    assert_eq!(decode(&[0xD8, 0x3D], "Char"), json!("\\uD83D"));
}

#[test]
fn char_array_value() {
    // CharPrimitiveArraySerializer: writeInt(length), then writeChar per element.
    let bytes = [0, 0, 0, 3, 0x00, 0x58, 0x00, 0x59, 0x00, 0x5A];
    assert_eq!(decode(&bytes, "char[]"), json!(["X", "Y", "Z"]));
    assert_eq!(decode(&[0, 0, 0, 0], "char[]"), json!([]));
}

#[test]
fn emoji_string_value() {
    // StringValue.writeString of "a😀b": length 4 + 1, then the units a, D83D, DE00, b.
    let bytes = [0x05, 0x61, 0xBD, 0xB0, 0x03, 0x80, 0xBC, 0x03, 0x62];
    assert_eq!(decode(&bytes, "String"), json!("a😀b"));
}

#[test]
fn null_string_is_null_not_empty() {
    // StringSerializer writes a null String as length 0, an empty one as length 1.
    assert_eq!(decode(&[0x00], "String"), Value::Null);
    assert_eq!(decode(&[0x01], "String"), json!(""));
    // TupleSerializer has no null flags, so a null String field is that same 0.
    let tuple = [0x00, 0, 0, 0, 0, 0, 0, 0, 5];
    assert_eq!(decode(&tuple, "Tuple<String,Long>"), json!([null, 5]));
    // StringArraySerializer writes each element with StringValue.writeString, nulls included.
    let array = [0, 0, 0, 3, 0x02, 0x61, 0x00, 0x01];
    assert_eq!(decode(&array, "String[]"), json!(["a", null, ""]));
}

#[test]
fn pojo_char_fields_do_not_stop_decoding() {
    let info = pojo(
        "Letters",
        &[("charBoxed", "Char"), ("charField", "Char"), ("n", "Int")],
    );
    let bytes = pojo_bytes(&[field(&[0x00, 0x5A]), field(&[0x00, 0x41]), field(&INT_7)]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&info), None),
        json!({ "charBoxed": "Z", "charField": "A", "n": 7 })
    );
}

#[test]
fn fields_after_an_undecodable_one_are_marked_not_null() {
    let info = pojo(
        "Mixed",
        &[
            ("a", "Int"),
            ("blob", "KryoSerializer"),
            ("other", "SomethingSerializer"),
            ("z", "Int"),
        ],
    );
    // A Kryo blob has no length prefix: the bytes after it cannot be located.
    let bytes = pojo_bytes(&[field(&INT_7), field(&[0x01, 0x02, 0x03]), field(&INT_7)]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&info), None),
        json!({
            "a": 7,
            "blob": "[Kryo]",
            "other": "[not decoded: follows blob]",
            "z": "[not decoded: follows blob]",
        })
    );

    // An unknown serializer is named in its marker.
    let info = pojo("Unknown", &[("odd", "BigIntSerializer"), ("z", "Int")]);
    let bytes = pojo_bytes(&[field(&[0x01]), field(&INT_7)]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&info), None),
        json!({ "odd": "[undecodable: BigIntSerializer]", "z": "[not decoded: follows odd]" })
    );
}

#[test]
fn a_null_field_stays_null() {
    let info = pojo("Nulls", &[("s", "String"), ("n", "Int")]);
    // writeBoolean(true) for a null field, and nothing else.
    let bytes = pojo_bytes(&[vec![0x01], field(&INT_7)]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&info), None),
        json!({ "s": null, "n": 7 })
    );
    // A null POJO: flags IS_NULL.
    assert_eq!(
        decode_state_value(&[0x01], "Pojo", Some(&info), None),
        Value::Null
    );
}

#[test]
fn an_undecodable_nested_field_stops_the_outer_pojo_too() {
    let inner = pojo(
        "Inner",
        &[("x", "Int"), ("k", "KryoSerializer"), ("y", "Int")],
    );
    let mut outer = pojo("Outer", &[("inner", "Inner"), ("after", "Int")]);
    outer.fields[0].nested_pojo = Some(Box::new(inner));
    let inner_bytes = pojo_bytes(&[field(&INT_7), field(&[0xAA, 0xBB])]);
    let bytes = pojo_bytes(&[field(&inner_bytes), field(&INT_7)]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&outer), None),
        json!({
            "inner": { "x": 7, "k": "[Kryo]", "y": "[not decoded: follows k]" },
            "after": "[not decoded: follows inner]",
        })
    );
}

#[test]
fn a_list_field_stopped_at_an_element_says_how_many_are_left() {
    // ListSerializer: writeInt(size), then each element.
    let info = pojo("Holder", &[("items", "List<KryoSerializer>"), ("z", "Int")]);
    let bytes = pojo_bytes(&[field(&[0, 0, 0, 3, 0x01]), field(&INT_7)]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&info), None),
        json!({
            "items": ["[Kryo]", "[not decoded: 2 more elements]"],
            "z": "[not decoded: follows items]",
        })
    );
}

#[test]
fn tuple_fields_after_an_undecodable_one_are_marked() {
    let bytes = [0x01, 0x02];
    assert_eq!(
        decode(&bytes, "Tuple<Kryo,Long,Int>"),
        json!([
            "[Kryo]",
            "[not decoded: follows f0]",
            "[not decoded: follows f0]"
        ])
    );
}

#[test]
fn a_subclass_instance_is_marked_not_misread() {
    // IS_SUBCLASS (4): writeUTF(className), then the subclass's own serializer — whose layout
    // this snapshot does not describe (PojoSerializer.java:382-386, 405-410).
    let info = pojo("Base", &[("n", "Int")]);
    let mut bytes = vec![0x04, 0x00, 0x05];
    bytes.extend_from_slice(b"a.Sub");
    bytes.extend_from_slice(&[0x02, 0x00, 0, 0, 0, 7]);
    assert_eq!(
        decode_state_value(&bytes, "Pojo", Some(&info), None),
        json!("[undecodable: subclass a.Sub of Base]")
    );
    // IS_TAGGED_SUBCLASS (8): writeByte(tag).
    assert_eq!(
        decode_state_value(&[0x08, 0x03, 0x02], "Pojo", Some(&info), None),
        json!("[undecodable: registered subclass #3 of Base]")
    );
}

#[test]
fn undecodable_state_values_are_markers_not_null() {
    assert_eq!(decode(&[0x01, 0x02, 0x03], "Kryo"), json!("[Kryo]"));
    assert_eq!(
        decode(&[0x01], "SomethingSerializer"),
        json!("[undecodable: SomethingSerializer]")
    );
    // Bytes that end before the value does.
    assert_eq!(decode(&[0x00, 0x00], "Long"), json!("[undecodable: Long]"));
    // A list state whose element type is unknown says so instead of being empty.
    assert_eq!(
        decode(&[0x01, 0x02], "List<Kryo>"),
        json!(["[Kryo]", "[not decoded: the rest of the list]"])
    );
}

#[test]
fn an_undecodable_map_key_is_a_marker_kept_unique() {
    // Key bytes: VoidNamespace (1 byte) + the key; value: NullableSerializer isNull=false + Long.
    let value = [0x00, 0, 0, 0, 0, 0, 0, 0, 9];
    let (key, v) = decode_map_entry(&[0x00, 0xAB], &value, "Kryo", "Long", None, None, 3);
    assert_eq!(key, "[Kryo] #3");
    assert_eq!(v, json!(9));
    // A null map value is a real null.
    let (_, v) = decode_map_entry(&[0x00, 0, 0, 0, 1], &[0x01], "Int", "Long", None, None, 0);
    assert_eq!(v, Value::Null);
}

#[test]
fn the_tui_rendering_shows_markers_apart_from_strings() {
    // Field names in Flink's (sorted) order, which is also the order serde_json keeps them in.
    let info = pojo(
        "Shown",
        &[
            ("a", "Char"),
            ("b", "String"),
            ("c", "KryoSerializer"),
            ("d", "Int"),
        ],
    );
    let bytes = pojo_bytes(&[
        field(&[0x00, 0x41]),
        field(&[0x03, b'h', b'i']),
        field(&[0x01]),
        field(&INT_7),
    ]);
    assert_eq!(
        deserialize_pojo_to_json(&bytes, &info, None),
        r#"{ a: "A", b: "hi", c: [Kryo], d: [not decoded: follows c] }"#
    );
    assert_eq!(
        value_to_display(&json!(["x", "[not decoded: 2 more elements]"])),
        r#"["x", [not decoded: 2 more elements]]"#
    );
    assert!(is_marker("[undecodable: Map<String,Int>]"));
    assert!(!is_marker("[3 bytes]"));
}

#[test]
fn the_tui_field_reader_reports_a_lost_position() {
    // `read_field_value_pub` is what the TUI's per-field POJO view reads with: `Err` tells it
    // the fields after this one cannot be located.
    let mut r = JavaReader::new(&[0x00, 0x41][..]);
    assert_eq!(
        read_field_value_pub(&mut r, "Char", None, None),
        Ok("\"A\"".to_string())
    );
    let mut r = JavaReader::new(&[0x01, 0x02][..]);
    assert_eq!(
        read_field_value_pub(&mut r, "KryoSerializer", None, None),
        Err("[Kryo]".to_string())
    );
    // A nested POJO that stopped part-way is an `Err` too, with what was read.
    let inner = pojo("Inner", &[("x", "Int"), ("k", "KryoSerializer")]);
    let bytes = pojo_bytes(&[field(&INT_7), field(&[0xAA])]);
    let mut r = JavaReader::new(&bytes[..]);
    assert_eq!(
        decode_pojo(&mut r, &inner, None),
        Err(json!({ "x": 7, "k": "[Kryo]" }))
    );
}

#[test]
fn a_keyed_list_stopped_at_an_element_says_the_rest_is_not_decoded() {
    // ListDelimitedSerializer: elements separated by ','. A Kryo element has no known length,
    // so the list cannot go on; it must not look like a one-element list.
    let mut bytes = vec![1u8, 2, 3];
    bytes.push(b',');
    bytes.extend_from_slice(&[9, 9, 9]);
    let v = decode(&bytes, "List<Kryo>");
    let items = v.as_array().expect("an array");
    assert_eq!(items[0], json!("[Kryo]"));
    assert_eq!(
        items.last().unwrap(),
        &json!("[not decoded: the rest of the list]")
    );
}

#[test]
fn a_null_map_key_does_not_read_as_the_string_null() {
    // VoidNamespace byte, then StringValue null (length 0).
    let (key, _) = flink_explorer::parser::value::decode_map_entry(
        &[0, 0],
        &[0, 0, 0, 0, 0, 0, 0, 0, 7],
        "String",
        "Long",
        None,
        None,
        0,
    );
    assert_eq!(key, "[null]");
}
