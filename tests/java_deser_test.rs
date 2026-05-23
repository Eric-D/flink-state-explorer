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
