use std::io::Read;

use crate::error::ParseError;

/// A reader wrapping a byte stream that tracks position for error reporting.
pub struct JavaReader<R> {
    inner: R,
    pos: u64,
}

impl<R: Read> JavaReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, pos: 0 }
    }

    pub fn position(&self) -> u64 {
        self.pos
    }

    fn read_exact_bytes(&mut self, buf: &mut [u8]) -> Result<(), ParseError> {
        self.inner
            .read_exact(buf)
            .map_err(|_| ParseError::UnexpectedEof(self.pos))?;
        self.pos += buf.len() as u64;
        Ok(())
    }

    /// Read a single byte.
    pub fn read_byte(&mut self) -> Result<u8, ParseError> {
        let mut buf = [0u8; 1];
        self.read_exact_bytes(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a boolean (1 byte, 0 = false, nonzero = true).
    pub fn read_boolean(&mut self) -> Result<bool, ParseError> {
        Ok(self.read_byte()? != 0)
    }

    /// Read a big-endian signed 16-bit short.
    pub fn read_short(&mut self) -> Result<i16, ParseError> {
        let mut buf = [0u8; 2];
        self.read_exact_bytes(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }

    /// Read a big-endian signed 32-bit integer.
    pub fn read_int(&mut self) -> Result<i32, ParseError> {
        let mut buf = [0u8; 4];
        self.read_exact_bytes(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    /// Read a big-endian signed 64-bit long.
    pub fn read_long(&mut self) -> Result<i64, ParseError> {
        let mut buf = [0u8; 8];
        self.read_exact_bytes(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    /// Read a Java modified UTF-8 string (2-byte length prefix, then bytes).
    pub fn read_utf(&mut self) -> Result<String, ParseError> {
        let len = {
            let mut buf = [0u8; 2];
            self.read_exact_bytes(&mut buf)?;
            u16::from_be_bytes(buf) as usize
        };
        let mut buf = vec![0u8; len];
        self.read_exact_bytes(&mut buf)?;
        let offset = self.pos - len as u64;
        String::from_utf8(buf).map_err(|e| ParseError::InvalidUtf8 { offset, source: e })
    }

    /// Read a Flink StringValue string (variable-length encoded).
    ///
    /// Format: length is (charCount + 1) encoded as a variable-length int
    /// (7 bits per byte, high bit = continuation). Zero means null.
    /// Each character is then encoded as a variable-length byte sequence
    /// (ASCII 0x01–0x7F → 1 byte, others → multi-byte).
    pub fn read_string_value(&mut self) -> Result<String, ParseError> {
        // Read variable-length encoded length (= charCount + 1, or 0 for null)
        let len_plus_one = self.read_vint()? as usize;
        if len_plus_one == 0 {
            return Ok(String::new()); // null → empty string for our purposes
        }
        let char_count = len_plus_one - 1;

        // Read char_count characters, each variable-length encoded
        let mut result = String::with_capacity(char_count);
        for _ in 0..char_count {
            let c = self.read_vint()?;
            if let Some(ch) = char::from_u32(c as u32) {
                result.push(ch);
            }
        }
        Ok(result)
    }

    /// Read a variable-length encoded integer (7 bits per byte, high bit = more).
    pub fn read_vint(&mut self) -> Result<i32, ParseError> {
        let first = self.read_byte()? as i32;
        if first & 0x80 == 0 {
            return Ok(first);
        }
        let mut value = first & 0x7f;
        let mut shift = 7;
        loop {
            let b = self.read_byte()? as i32;
            value |= (b & 0x7f) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 28 {
                return Err(ParseError::UnexpectedEof(self.pos));
            }
        }
        Ok(value)
    }

    /// Read a fixed number of raw bytes.
    pub fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, ParseError> {
        let mut buf = vec![0u8; len];
        self.read_exact_bytes(&mut buf)?;
        Ok(buf)
    }

    /// Skip a given number of bytes.
    pub fn skip(&mut self, len: usize) -> Result<(), ParseError> {
        let mut buf = vec![0u8; len];
        self.read_exact_bytes(&mut buf)?;
        Ok(())
    }
}
