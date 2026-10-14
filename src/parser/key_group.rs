//! Flink's key → key-group → subtask assignment, and the Java `hashCode()` of the key types the
//! parser decodes, so a key typed as text can be located without reading the savepoint.
//!
//! Verified against apache/flink `release-1.20`:
//! - `MathUtils.murmurHash` / `bitMix` (flink-core `util/MathUtils.java:137-155`);
//! - `KeyGroupRangeAssignment.assignToKeyGroup` = `computeKeyGroupForKeyHash(key.hashCode(),
//!   maxParallelism)` = `murmurHash(keyHash) % maxParallelism` (flink-runtime
//!   `state/KeyGroupRangeAssignment.java:63-77`);
//! - `computeKeyGroupRangeForOperatorIndex` (`:93-106`) and `computeOperatorIndexForKeyGroup`
//!   (`:124-127`);
//! - the `hashCode()` of `String` (over UTF-16 code units), the boxed primitives, `BigInteger`,
//!   and Flink's `TupleN` (`31 * result + field.hashCode()`, e.g. `Tuple3.java:181-186`).
//!
//! [`JavaKey`] turns the text a key renders as ([`crate::parser::value::decode_key`]) back into the
//! typed value, its `hashCode()` and its serialized bytes; [`JavaKey::canonical_text`] renders
//! those bytes through the decoder itself, so a lookup compares keys exactly as the parser renders
//! them.

/// `MathUtils.bitMix`.
fn bit_mix(mut i: i32) -> i32 {
    i ^= ((i as u32) >> 16) as i32;
    i = i.wrapping_mul(0x85eb_ca6bu32 as i32);
    i ^= ((i as u32) >> 13) as i32;
    i = i.wrapping_mul(0xc2b2_ae35u32 as i32);
    i ^= ((i as u32) >> 16) as i32;
    i
}

/// `MathUtils.murmurHash`: never negative.
pub fn murmur_hash(mut code: i32) -> i32 {
    code = code.wrapping_mul(0xcc9e_2d51u32 as i32);
    code = code.rotate_left(15);
    code = code.wrapping_mul(0x1b87_3593);
    code = code.rotate_left(13);
    code = code.wrapping_mul(5).wrapping_add(0xe654_6b64u32 as i32);
    code ^= 4;
    code = bit_mix(code);
    if code >= 0 {
        code
    } else if code != i32::MIN {
        -code
    } else {
        0
    }
}

/// `KeyGroupRangeAssignment.computeKeyGroupForKeyHash`: the key-group of a key whose
/// `hashCode()` is `key_hash`.
pub fn key_group_for_hash(key_hash: i32, max_parallelism: i32) -> i32 {
    murmur_hash(key_hash) % max_parallelism
}

/// `KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex`: the inclusive `(start, end)`
/// key-groups of `subtask`.
pub fn range_for_subtask(max_parallelism: i32, parallelism: i32, subtask: i32) -> (i32, i32) {
    let start = (subtask * max_parallelism + parallelism - 1) / parallelism;
    let end = ((subtask + 1) * max_parallelism - 1) / parallelism;
    (start, end)
}

/// `KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup`: the subtask owning `key_group`.
pub fn subtask_for_key_group(max_parallelism: i32, parallelism: i32, key_group: i32) -> i32 {
    key_group * parallelism / max_parallelism
}

/// `String.hashCode`: `31 * h + c` over the UTF-16 code units.
pub fn java_string_hash(s: &str) -> i32 {
    java_utf16_hash(s.encode_utf16())
}

/// `String.hashCode` of a string given as its UTF-16 code units (lone surrogates included).
pub fn java_utf16_hash(units: impl IntoIterator<Item = u16>) -> i32 {
    units
        .into_iter()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

/// `java.util.Date.hashCode`: `(int) ht ^ (int) (ht >> 32)` of `getTime()` (JDK 21
/// `java/util/Date.java`), the same low 32 bits as `Long.hashCode`. `java.sql.Date` inherits it.
pub fn java_date_hash(millis: i64) -> i32 {
    (millis as i32) ^ ((millis >> 32) as i32)
}

/// `java.time.LocalDate.hashCode` (JDK 21 `java/time/LocalDate.java`):
/// `(year & 0xFFFFF800) ^ ((year << 11) + (month << 6) + day)`.
pub fn java_local_date_hash(year: i32, month: i32, day: i32) -> i32 {
    (year & 0xFFFF_F800u32 as i32)
        ^ (year
            .wrapping_shl(11)
            .wrapping_add(month.wrapping_shl(6))
            .wrapping_add(day))
}

/// `Long.hashCode`: `(int) (value ^ (value >>> 32))`.
pub fn java_long_hash(v: i64) -> i32 {
    (v ^ ((v as u64) >> 32) as i64) as i32
}

/// `Boolean.hashCode`.
pub fn java_boolean_hash(v: bool) -> i32 {
    if v {
        1231
    } else {
        1237
    }
}

/// `Float.floatToIntBits`: the raw bits, every NaN collapsed to the canonical one. Also what
/// `FloatSerializer` writes (`writeFloat`).
pub fn java_float_bits(f: f32) -> i32 {
    if f.is_nan() {
        0x7fc0_0000
    } else {
        f.to_bits() as i32
    }
}

/// `Double.doubleToLongBits`: the raw bits, every NaN collapsed to the canonical one.
pub fn java_double_bits(d: f64) -> i64 {
    if d.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        d.to_bits() as i64
    }
}

/// `Float.hashCode` = `floatToIntBits`.
pub fn java_float_hash(f: f32) -> i32 {
    java_float_bits(f)
}

/// `Double.hashCode` = `Long.hashCode(doubleToLongBits)`.
pub fn java_double_hash(d: f64) -> i32 {
    java_long_hash(java_double_bits(d))
}

/// `TupleN.hashCode`: `31 * result + f.hashCode()` over the fields, starting from 0.
pub fn java_tuple_hash(field_hashes: &[i32]) -> i32 {
    field_hashes
        .iter()
        .fold(0i32, |h, &f| h.wrapping_mul(31).wrapping_add(f))
}

/// `BigInteger.hashCode`: `(int)(31 * h + (mag[i] & LONG_MASK))` over the magnitude's big-endian
/// ints (no leading zero int), times the signum.
pub fn java_big_integer_hash(negative: bool, magnitude: &[u32]) -> i32 {
    let words = &magnitude[magnitude.iter().take_while(|&&w| w == 0).count()..];
    if words.is_empty() {
        return 0;
    }
    let h = words
        .iter()
        .fold(0i32, |h, &w| h.wrapping_mul(31).wrapping_add(w as i32));
    if negative {
        h.wrapping_neg()
    } else {
        h
    }
}

/// A key value of one of the types whose `hashCode()` is known, parsed from its rendered text.
#[derive(Debug, Clone, PartialEq)]
pub enum JavaKey {
    String(String),
    /// A `String` holding a lone surrogate, as its UTF-16 code units (no Rust string can hold
    /// it); rendered escaped by [`crate::parser::value::key_string`].
    Utf16(Vec<u16>),
    Long(i64),
    Int(i32),
    Short(i16),
    Byte(i8),
    /// One UTF-16 code unit.
    Char(u16),
    Boolean(bool),
    Float(f32),
    Double(f64),
    /// Sign and big-endian magnitude words (no leading zero word; empty = zero).
    BigInteger {
        negative: bool,
        magnitude: Vec<u32>,
    },
    Tuple(Vec<JavaKey>),
    /// A `java.util.Date` / `java.sql.Date` (`DateSerializer` / `SqlDateSerializer`): epoch millis.
    Date(i64),
    /// A `java.time.LocalDate` (`LocalDateSerializer`).
    LocalDate {
        year: i32,
        month: u8,
        day: u8,
    },
}

impl JavaKey {
    /// Parse `text` — a key as [`crate::parser::value::decode_key`] renders it — as a key of the
    /// serializer type `key_type` (`String`, `Long`, `Tuple<String,Long>`, …).
    ///
    /// Accepted forms: the rendered form itself; for numbers, anything Rust parses (`5`, `-0.0`,
    /// `NaN`, `inf`); for `Float`/`Double`, also the `0x` + hex of the serialized bytes that a
    /// non-finite value renders as; for `Char`, one character or a `\uXXXX` escape; for a tuple,
    /// a JSON array of its fields. `Err` explains why the text is not such a key, or why the type
    /// has no known `hashCode()` (POJO, Kryo, `Timestamp`, …); `Date` takes its rendering or epoch
    /// milliseconds, `LocalDate` its rendering, a `String` key rendered escaped is unescaped.
    pub fn parse(key_type: &str, text: &str) -> Result<JavaKey, String> {
        let bad = || format!("{:?} is not a {} key", text, key_type);
        Ok(match key_type {
            "String" => {
                let units = crate::parser::value::parse_key_string(text).ok_or_else(|| {
                    format!(
                        "{:?} is not a String key: in a key rendered escaped, \\u must start a \
                         lone-surrogate escape \\uXXXX and a literal backslash is \\\\",
                        text
                    )
                })?;
                match String::from_utf16(&units) {
                    Ok(s) => JavaKey::String(s),
                    Err(_) => JavaKey::Utf16(units),
                }
            }
            "Date" => JavaKey::Date(match text.trim().parse::<i64>() {
                Ok(millis) => millis,
                Err(_) => parse_iso_millis(text).ok_or_else(|| {
                    format!(
                        "{:?} is not a Date key: give it as rendered \
                         (YYYY-MM-DDTHH:MM:SS[.mmm]Z, UTC) or as epoch milliseconds",
                        text
                    )
                })?,
            }),
            "LocalDate" => {
                let (year, month, day) = parse_local_date(text)
                    .ok_or_else(|| format!("{:?} is not a LocalDate key (YYYY-MM-DD)", text))?;
                JavaKey::LocalDate { year, month, day }
            }
            "Long" => JavaKey::Long(text.trim().parse().map_err(|_| bad())?),
            "Int" => JavaKey::Int(text.trim().parse().map_err(|_| bad())?),
            "Short" => JavaKey::Short(text.trim().parse().map_err(|_| bad())?),
            "Byte" => JavaKey::Byte(text.trim().parse().map_err(|_| bad())?),
            "Boolean" => match text.trim() {
                "true" => JavaKey::Boolean(true),
                "false" => JavaKey::Boolean(false),
                _ => return Err(bad()),
            },
            "Char" => JavaKey::Char(parse_char(text).ok_or_else(bad)?),
            "Float" => JavaKey::Float(match hex_bytes::<4>(text) {
                Some(b) => f32::from_bits(u32::from_be_bytes(b)),
                None => text.trim().parse().map_err(|_| bad())?,
            }),
            "Double" => JavaKey::Double(match hex_bytes::<8>(text) {
                Some(b) => f64::from_bits(u64::from_be_bytes(b)),
                None => text.trim().parse().map_err(|_| bad())?,
            }),
            "BigInteger" => {
                let (negative, magnitude) = parse_decimal(text.trim()).ok_or_else(bad)?;
                JavaKey::BigInteger {
                    negative,
                    magnitude,
                }
            }
            t if t.starts_with("Tuple<") && t.ends_with('>') => {
                let field_types =
                    crate::parser::value::split_top_level(&t["Tuple<".len()..t.len() - 1]);
                let items: Vec<serde_json::Value> = match serde_json::from_str(text) {
                    Ok(serde_json::Value::Array(items)) => items,
                    _ => {
                        return Err(format!(
                            "{:?} is not a {} key: a tuple key is a JSON array of its fields",
                            text, key_type
                        ))
                    }
                };
                if items.len() != field_types.len() {
                    return Err(format!(
                        "{:?} has {} fields, {} has {}",
                        text,
                        items.len(),
                        key_type,
                        field_types.len()
                    ));
                }
                let mut fields = Vec::with_capacity(items.len());
                for (item, ft) in items.iter().zip(field_types) {
                    let field_text = match item {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => {
                            return Err(format!("{:?}: a tuple field cannot be null", text))
                        }
                        other => other.to_string(),
                    };
                    fields.push(JavaKey::parse(ft.trim(), &field_text)?);
                }
                JavaKey::Tuple(fields)
            }
            "Pojo" => {
                return Err(
                    "a POJO key is hashed by its class's own hashCode(), which the \
                            savepoint does not describe"
                        .to_string(),
                )
            }
            other => {
                return Err(format!(
                    "the hashCode() of a {} key is not known here",
                    other
                ))
            }
        })
    }

    /// The Java `hashCode()` of the key: what `KeyGroupRangeAssignment.assignToKeyGroup` hashes.
    pub fn hash_code(&self) -> i32 {
        match self {
            JavaKey::String(s) => java_string_hash(s),
            JavaKey::Utf16(units) => java_utf16_hash(units.iter().copied()),
            JavaKey::Date(millis) => java_date_hash(*millis),
            JavaKey::LocalDate { year, month, day } => {
                java_local_date_hash(*year, *month as i32, *day as i32)
            }
            JavaKey::Long(v) => java_long_hash(*v),
            JavaKey::Int(v) => *v,
            // Short.hashCode / Byte.hashCode / Character.hashCode: `(int) value`.
            JavaKey::Short(v) => *v as i32,
            JavaKey::Byte(v) => *v as i32,
            JavaKey::Char(v) => *v as i32,
            JavaKey::Boolean(v) => java_boolean_hash(*v),
            JavaKey::Float(v) => java_float_hash(*v),
            JavaKey::Double(v) => java_double_hash(*v),
            JavaKey::BigInteger {
                negative,
                magnitude,
            } => java_big_integer_hash(*negative, magnitude),
            JavaKey::Tuple(fields) => {
                java_tuple_hash(&fields.iter().map(JavaKey::hash_code).collect::<Vec<_>>())
            }
        }
    }

    /// The key-group Flink assigns this key to.
    pub fn key_group(&self, max_parallelism: i32) -> i32 {
        key_group_for_hash(self.hash_code(), max_parallelism)
    }

    /// The bytes the key's Flink serializer writes (`StringSerializer` = `StringValue.writeString`,
    /// the boxed primitives big-endian, `BigIntSerializer`, `TupleSerializer` = the fields back to
    /// back).
    pub fn serialize(&self, out: &mut Vec<u8>) {
        match self {
            JavaKey::String(s) => write_string_value(out, s),
            JavaKey::Utf16(units) => write_utf16_value(out, units),
            // DateSerializer.serialize: `writeLong(record.getTime())`.
            JavaKey::Date(millis) => out.extend_from_slice(&millis.to_be_bytes()),
            // LocalDateSerializer.serialize: `writeInt(year)`, `writeByte(month)`, `writeByte(day)`.
            JavaKey::LocalDate { year, month, day } => {
                out.extend_from_slice(&year.to_be_bytes());
                out.push(*month);
                out.push(*day);
            }
            JavaKey::Long(v) => out.extend_from_slice(&v.to_be_bytes()),
            JavaKey::Int(v) => out.extend_from_slice(&v.to_be_bytes()),
            JavaKey::Short(v) => out.extend_from_slice(&v.to_be_bytes()),
            JavaKey::Byte(v) => out.push(*v as u8),
            JavaKey::Char(v) => out.extend_from_slice(&v.to_be_bytes()),
            JavaKey::Boolean(v) => out.push(*v as u8),
            JavaKey::Float(v) => out.extend_from_slice(&java_float_bits(*v).to_be_bytes()),
            JavaKey::Double(v) => out.extend_from_slice(&java_double_bits(*v).to_be_bytes()),
            // BigIntSerializer.writeBigInteger (BigIntSerializer.java:87-112): `writeInt(len + 4)`
            // then `toByteArray()`. (The 1/2/3 shortcuts are for the ZERO/ONE/TEN instances only,
            // and decode to the same value.)
            JavaKey::BigInteger {
                negative,
                magnitude,
            } => {
                let bytes = big_integer_to_byte_array(*negative, magnitude);
                out.extend_from_slice(&(bytes.len() as i32 + 4).to_be_bytes());
                out.extend_from_slice(&bytes);
            }
            JavaKey::Tuple(fields) => fields.iter().for_each(|f| f.serialize(out)),
        }
    }

    /// The key as the parser renders it: its serialized bytes through
    /// [`crate::parser::value::decode_key`], the very function that renders keys read from a
    /// savepoint. `None` if the decoder does not read `key_type`.
    pub fn canonical_text(&self, key_type: &str) -> Option<String> {
        let mut bytes = Vec::new();
        self.serialize(&mut bytes);
        crate::parser::value::decode_key(&bytes, key_type, None)
            .filter(|&(_, used)| used == bytes.len())
            .map(|(text, _)| text)
    }
}

/// One UTF-16 unit: a single BMP character, or the `\uXXXX` escape a lone surrogate renders as.
fn parse_char(text: &str) -> Option<u16> {
    let mut chars = text.chars();
    if let (Some(c), None) = (chars.next(), chars.next()) {
        return u16::try_from(c as u32).ok();
    }
    let hex = text.strip_prefix("\\u")?;
    (hex.len() == 4)
        .then(|| u16::from_str_radix(hex, 16).ok())
        .flatten()
}

/// `0x` + exactly `N` bytes of hex.
fn hex_bytes<const N: usize>(text: &str) -> Option<[u8; N]> {
    let hex = text.trim().strip_prefix("0x")?;
    if hex.len() != N * 2 || !hex.is_ascii() {
        return None;
    }
    let mut out = [0u8; N];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// A decimal integer as (negative, big-endian u32 magnitude without leading zero words).
fn parse_decimal(text: &str) -> Option<(bool, Vec<u32>)> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Little-endian words while accumulating.
    let mut words: Vec<u32> = Vec::new();
    for d in digits.bytes() {
        let mut carry = (d - b'0') as u64;
        for w in words.iter_mut() {
            let v = *w as u64 * 10 + carry;
            *w = v as u32;
            carry = v >> 32;
        }
        if carry != 0 {
            words.push(carry as u32);
        }
    }
    words.reverse();
    let negative = negative && !words.is_empty();
    Some((negative, words))
}

/// `BigInteger.toByteArray()`: big-endian two's complement in the minimum number of bytes,
/// including at least one sign bit.
fn big_integer_to_byte_array(negative: bool, magnitude: &[u32]) -> Vec<u8> {
    // One extra zero byte leaves room for the sign bit.
    let mut bytes = vec![0u8];
    bytes.extend(magnitude.iter().flat_map(|w| w.to_be_bytes()));
    if negative {
        // Two's complement: invert, then add one.
        for b in bytes.iter_mut() {
            *b = !*b;
        }
        for b in bytes.iter_mut().rev() {
            let (v, overflow) = b.overflowing_add(1);
            *b = v;
            if !overflow {
                break;
            }
        }
    }
    let mut start = 0;
    while start < bytes.len() - 1 {
        let redundant = (bytes[start] == 0x00 && bytes[start + 1] & 0x80 == 0)
            || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0);
        if !redundant {
            break;
        }
        start += 1;
    }
    bytes[start..].to_vec()
}

/// `StringValue.writeString`: var-length `charCount + 1`, then each UTF-16 unit var-length.
fn write_string_value(out: &mut Vec<u8>, s: &str) {
    let units: Vec<u16> = s.encode_utf16().collect();
    write_utf16_value(out, &units);
}

fn write_utf16_value(out: &mut Vec<u8>, units: &[u16]) {
    write_var(out, units.len() as u32 + 1);
    for &u in units {
        write_var(out, u as u32);
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's `days_from_civil`, the
/// inverse of the conversion in [`crate::parser::pojo::millis_to_iso`]).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DD`, the year possibly negative or longer than 4 digits: split from the right.
fn split_date(text: &str) -> Option<(&str, &str, &str)> {
    let mut parts = text.rsplitn(3, '-');
    let d = parts.next()?;
    let m = parts.next()?;
    let y = parts.next()?;
    Some((y, m, d))
}

/// The epoch millis of a Date key as `millis_to_iso` renders it. Only an exact rendering is
/// accepted (it is re-rendered and compared), so `2020-02-30T…` is refused rather than read as
/// another day.
fn parse_iso_millis(text: &str) -> Option<i64> {
    let text = text.trim();
    let (date, time) = text.strip_suffix('Z')?.split_once('T')?;
    let (y, m, d) = split_date(date)?;
    let (hms, ms) = match time.split_once('.') {
        Some((hms, ms)) => (hms, ms.parse::<i64>().ok()?),
        None => (time, 0),
    };
    let mut hms = hms.split(':');
    let (h, mi, s) = (hms.next()?, hms.next()?, hms.next()?);
    let num = |t: &str| t.parse::<i64>().ok().filter(|v| v.abs() < 1 << 40);
    let days = days_from_civil(num(y)?, num(m)?, num(d)?);
    let secs = days
        .checked_mul(86_400)?
        .checked_add(num(h)? * 3600 + num(mi)? * 60 + num(s)?)?;
    let millis = secs.checked_mul(1000)?.checked_add(ms)?;
    (crate::parser::pojo::millis_to_iso(millis) == text).then_some(millis)
}

/// A LocalDate key as `decode_field` renders it (`{:04}-{:02}-{:02}`), exactly.
fn parse_local_date(text: &str) -> Option<(i32, u8, u8)> {
    let text = text.trim();
    let (y, m, d) = split_date(text)?;
    let (y, m, d) = (
        y.parse::<i32>().ok()?,
        m.parse::<u8>().ok()?,
        d.parse::<u8>().ok()?,
    );
    (format!("{:04}-{:02}-{:02}", y, m, d) == text).then_some((y, m, d))
}

fn write_var(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values computed with Java 21 (`"hello".hashCode()` …); the fuller table, with
    // key-groups, is `tests/key_group_test.rs`.
    #[test]
    fn hash_codes_match_java() {
        assert_eq!(java_string_hash(""), 0);
        assert_eq!(java_string_hash("hello"), 99_162_322);
        assert_eq!(java_string_hash("key-0000000"), 860_643_522);
        // Over UTF-16 units: U+1F600 is the surrogate pair D83D DE00.
        assert_eq!(java_string_hash("\u{1F600}"), 1_772_899);
        assert_eq!(java_long_hash(0), 0);
        assert_eq!(java_long_hash(-1), 0);
        assert_eq!(java_long_hash(1 << 32), 1);
        assert_eq!(java_long_hash(i64::MAX), i32::MIN);
        assert_eq!(java_boolean_hash(true), 1231);
        assert_eq!(java_boolean_hash(false), 1237);
        assert_eq!(java_float_hash(1.0), 0x3f80_0000);
        assert_eq!(java_float_hash(f32::NAN), 0x7fc0_0000);
        assert_eq!(java_double_hash(1.0), 0x3ff0_0000);
        assert_eq!(java_double_hash(-0.0), i32::MIN);
        assert_eq!(java_double_hash(f64::NAN), 0x7ff8_0000);
        // Tuple2.of("a", 1L).hashCode() = 31 * 97 + 1.
        assert_eq!(java_tuple_hash(&[97, 1]), 3008);
        // BigInteger.valueOf(-5).hashCode() = -5; 2^32 has mag [1, 0] → 31.
        assert_eq!(java_big_integer_hash(true, &[5]), -5);
        assert_eq!(java_big_integer_hash(false, &[1, 0]), 31);
        assert_eq!(java_big_integer_hash(false, &[]), 0);
    }

    // Printed by Java 21: `new java.util.Date(m).hashCode()`, `LocalDate.of(y, m, d).hashCode()`,
    // `("x" + (char) 0xD800).hashCode()`.
    #[test]
    fn date_and_lone_surrogate_hashes_match_java() {
        for (millis, h) in [
            (0i64, 0),
            (1, 1),
            (-1, 0),
            (1_700_000_000_123, -807_048_720),
            (-62_135_596_800_000, 304_934_780),
            (253_402_300_799_999, -769_704_568),
            (i64::MAX, i32::MIN),
        ] {
            assert_eq!(java_date_hash(millis), h, "{}", millis);
            assert_eq!(JavaKey::Date(millis).hash_code(), h);
        }
        for ((y, m, d), h) in [
            ((2024, 2, 29), 4_145_309),
            ((1970, 1, 1), 4_034_625),
            ((-5, 12, 31), 8_991),
            ((12345, 6, 7), 25_295_239),
            ((-999_999_999, 1, 1), -304_793_535),
            ((999_999_999, 12, 31), -304_794_849),
        ] {
            assert_eq!(java_local_date_hash(y, m, d), h, "{}-{}-{}", y, m, d);
        }
        assert_eq!(java_utf16_hash([0x78, 0xD800]), 59_016);
        let k = JavaKey::parse("String", "x\\uD800").unwrap();
        assert_eq!(k, JavaKey::Utf16(vec![0x78, 0xD800]));
        assert_eq!(k.hash_code(), 59_016);
        assert_eq!(k.canonical_text("String").unwrap(), "x\\uD800");
        // A literal backslash-u renders escaped, and parses back to itself.
        let lit = JavaKey::String("x\\uD800".to_string());
        assert_eq!(lit.canonical_text("String").unwrap(), "x\\\\uD800");
        assert_eq!(JavaKey::parse("String", "x\\\\uD800").unwrap(), lit);
        assert!(JavaKey::parse("String", "x\\uZZZZ").is_err());
        let d = JavaKey::parse("Date", "2023-11-14T22:13:20.123Z").unwrap();
        assert_eq!(d, JavaKey::Date(1_700_000_000_123));
        assert_eq!(
            d.canonical_text("Date").unwrap(),
            "2023-11-14T22:13:20.123Z"
        );
        assert!(JavaKey::parse("Date", "2023-11-14").is_err());
        let ld = JavaKey::parse("LocalDate", "-005-12-31").unwrap();
        assert_eq!(ld.hash_code(), 8_991);
        assert_eq!(ld.canonical_text("LocalDate").unwrap(), "-005-12-31");
    }

    #[test]
    fn murmur_and_ranges_match_flink() {
        for max_p in [1, 2, 128, 32768] {
            for h in [i32::MIN, -1, 0, 1, 42, 1600, i32::MAX] {
                let kg = key_group_for_hash(h, max_p);
                assert!((0..max_p).contains(&kg));
            }
        }
        // Every key-group's owner, by Flink's formula, has it in its range.
        for (max_p, p) in [(128, 1), (128, 2), (128, 3), (4096, 7), (32768, 1000)] {
            for kg in 0..max_p {
                let st = subtask_for_key_group(max_p, p, kg);
                let (s, e) = range_for_subtask(max_p, p, st);
                assert!(s <= kg && kg <= e, "kg {} subtask {}", kg, st);
            }
        }
    }

    #[test]
    fn keys_parse_and_render_canonically() {
        let k = JavaKey::parse("Long", "42").unwrap();
        assert_eq!(k.canonical_text("Long").unwrap(), "42");
        let k = JavaKey::parse("Double", "3").unwrap();
        assert_eq!(k.canonical_text("Double").unwrap(), "3.0");
        let nan = JavaKey::parse("Double", "NaN").unwrap();
        assert_eq!(nan.canonical_text("Double").unwrap(), "0x7ff8000000000000");
        let back = JavaKey::parse("Double", "0x7ff8000000000000").unwrap();
        assert_eq!(back.hash_code(), nan.hash_code());
        let k = JavaKey::parse("Tuple<String,Long>", r#"["a,b",7]"#).unwrap();
        assert_eq!(
            k.canonical_text("Tuple<String,Long>").unwrap(),
            r#"["a,b",7]"#
        );
        for v in [
            "0",
            "-1",
            "255",
            "-256",
            "4294967296",
            "-170141183460469231731687303715884105728",
        ] {
            let k = JavaKey::parse("BigInteger", v).unwrap();
            assert_eq!(k.canonical_text("BigInteger").unwrap(), v);
        }
        assert_eq!(
            JavaKey::parse("Char", "\\uD800").unwrap(),
            JavaKey::Char(0xD800)
        );
        assert!(JavaKey::parse("Long", "x").is_err());
        assert!(JavaKey::parse("Pojo", "{}").is_err());
    }
}
