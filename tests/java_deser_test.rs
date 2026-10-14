use flink_explorer::parser::java_deser::JavaReader;

#[test]
fn read_byte_valid() {
    let data = [0x42u8];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_byte().unwrap(), 0x42);
    assert_eq!(reader.position(), 1);
}

#[test]
fn read_byte_truncated() {
    let data: [u8; 0] = [];
    let mut reader = JavaReader::new(&data[..]);
    assert!(reader.read_byte().is_err());
}

#[test]
fn read_boolean_true_and_false() {
    let data = [1u8, 0u8];
    let mut reader = JavaReader::new(&data[..]);
    assert!(reader.read_boolean().unwrap());
    assert!(!reader.read_boolean().unwrap());
}

#[test]
fn read_short_valid() {
    // 0x0102 = 258
    let data = [0x01u8, 0x02];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_short().unwrap(), 258);
}

#[test]
fn read_short_negative() {
    // i16::MIN = -32768 = 0x8000
    let data = [0x80u8, 0x00];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_short().unwrap(), i16::MIN);
}

#[test]
fn read_short_truncated() {
    let data = [0x01u8];
    let mut reader = JavaReader::new(&data[..]);
    assert!(reader.read_short().is_err());
}

#[test]
fn read_int_valid() {
    // 0x00000001 = 1
    let data = [0x00u8, 0x00, 0x00, 0x01];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_int().unwrap(), 1);
}

#[test]
fn read_int_min_max() {
    let min_data = i32::MIN.to_be_bytes();
    let max_data = i32::MAX.to_be_bytes();
    let combined: Vec<u8> = [min_data, max_data].concat();
    let mut reader = JavaReader::new(&combined[..]);
    assert_eq!(reader.read_int().unwrap(), i32::MIN);
    assert_eq!(reader.read_int().unwrap(), i32::MAX);
}

#[test]
fn read_int_truncated() {
    let data = [0x00u8, 0x00, 0x01];
    let mut reader = JavaReader::new(&data[..]);
    assert!(reader.read_int().is_err());
}

#[test]
fn read_long_valid() {
    let data = 123456789i64.to_be_bytes();
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_long().unwrap(), 123456789);
}

#[test]
fn read_long_min_max() {
    let combined: Vec<u8> = [i64::MIN.to_be_bytes(), i64::MAX.to_be_bytes()].concat();
    let mut reader = JavaReader::new(&combined[..]);
    assert_eq!(reader.read_long().unwrap(), i64::MIN);
    assert_eq!(reader.read_long().unwrap(), i64::MAX);
}

#[test]
fn read_utf_empty_string() {
    // length = 0, no bytes
    let data = [0x00u8, 0x00];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_utf().unwrap(), "");
}

#[test]
fn read_utf_hello() {
    let s = "hello";
    let len = s.len() as u16;
    let mut data = Vec::new();
    data.extend_from_slice(&len.to_be_bytes());
    data.extend_from_slice(s.as_bytes());
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_utf().unwrap(), "hello");
}

#[test]
fn read_utf_truncated() {
    // says length 5 but only 2 bytes follow
    let data = [0x00u8, 0x05, b'h', b'i'];
    let mut reader = JavaReader::new(&data[..]);
    assert!(reader.read_utf().is_err());
}

#[test]
fn read_char_is_big_endian_utf16_unit() {
    // DataOutputSerializer.writeChar writes the high byte first.
    let data = [0x00u8, 0x41, 0xD8, 0x3D, 0x00];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_char().unwrap(), 0x0041);
    assert_eq!(reader.read_char().unwrap(), 0xD83D);
    assert_eq!(reader.position(), 4);
    assert!(reader.read_char().is_err(), "one byte left is not a char");
}

// StringValue bytes below are written by hand from `StringValue.writeString` (flink-core
// release-1.20, types/StringValue.java:798-850): var-length `charCount + 1`, then each UTF-16
// unit var-length — `c | 0x80`, `(c >>> 7) | 0x80`, `c >>> 14` for a unit >= 0x4000.

#[test]
fn read_string_value_keeps_surrogate_pairs() {
    // "a😀b": 4 UTF-16 units (U+1F600 is the pair D83D DE00), so the length byte is 5.
    let data = [0x05u8, 0x61, 0xBD, 0xB0, 0x03, 0x80, 0xBC, 0x03, 0x62];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_string_value().unwrap(), "a😀b");
    assert_eq!(reader.position(), data.len() as u64);
}

#[test]
fn read_string_value_two_byte_unit() {
    // "é" = U+00E9 → 0xE9 | 0x80, 0xE9 >>> 7.
    let data = [0x02u8, 0xE9, 0x01];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_string_value().unwrap(), "é");
}

#[test]
fn read_string_value_lone_surrogate_is_replacement_char() {
    // "x" + a lone high surrogate D800, which a Java String may hold and a Rust one cannot.
    let data = [0x03u8, 0x78, 0x80, 0xB0, 0x03];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_string_value().unwrap(), "x\u{FFFD}");
    assert_eq!(reader.position(), data.len() as u64);
}

#[test]
fn null_string_value_is_none_and_empty_is_some() {
    // writeString(null) writes a single 0; writeString("") writes length 0 + 1.
    let mut reader = JavaReader::new(&[0x00u8, 0x01][..]);
    assert_eq!(reader.read_nullable_string_value().unwrap(), None);
    assert_eq!(
        reader.read_nullable_string_value().unwrap(),
        Some(String::new())
    );
    // Where Flink never writes null (keys), the null reads as "".
    let mut reader = JavaReader::new(&[0x00u8][..]);
    assert_eq!(reader.read_string_value().unwrap(), "");
}

#[test]
fn read_bytes_valid() {
    let data = [0x01u8, 0x02, 0x03, 0x04];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_bytes(3).unwrap(), vec![0x01, 0x02, 0x03]);
    assert_eq!(reader.position(), 3);
}

#[test]
fn read_bytes_zero_length() {
    let data = [0x01u8];
    let mut reader = JavaReader::new(&data[..]);
    assert_eq!(reader.read_bytes(0).unwrap(), Vec::<u8>::new());
}

#[test]
fn position_tracks_correctly() {
    // int (4) + long (8) + utf("ab" = 2+2) = 16
    let mut data = Vec::new();
    data.extend_from_slice(&42i32.to_be_bytes());
    data.extend_from_slice(&99i64.to_be_bytes());
    data.extend_from_slice(&2u16.to_be_bytes());
    data.extend_from_slice(b"ab");
    let mut reader = JavaReader::new(&data[..]);
    let _ = reader.read_int().unwrap();
    assert_eq!(reader.position(), 4);
    let _ = reader.read_long().unwrap();
    assert_eq!(reader.position(), 12);
    let _ = reader.read_utf().unwrap();
    assert_eq!(reader.position(), 16);
}
