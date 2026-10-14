//! Parquet output for the streaming single-state export.
//!
//! The reading side is shared with NDJSON ([`crate::export_state::export_state_rows`]); this is
//! only a [`RowSink`]. Rows are buffered into Arrow arrays and flushed as row groups, so memory
//! stays bounded by the batch, not by the state.
//!
//! **The `value` column is typed per export.** A state has exactly one value serializer, and one
//! export covers one state, so the schema can be chosen from it: `Int64` for integral scalars,
//! `Float64`, `Boolean`, `Utf8` for strings — and `Utf8` holding compact JSON for everything
//! structured (POJO, Map, List, Tuple, arrays, Kryo). The `value_type` column carries the Flink
//! serializer type so a reader can tell which of the two a `Utf8` column is.

use std::path::Path;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

use crate::export_state::{RowSink, StateRow};

/// Rows per Arrow batch; also the row-group size handed to the Parquet writer.
const BATCH_ROWS: usize = 65_536;

/// How the state's values are stored in the `value` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueColumn {
    Int64,
    Float64,
    Boolean,
    Utf8,
    /// `Utf8`, holding compact JSON — the value is structured.
    Json,
}

impl ValueColumn {
    /// Pick a column type from a Flink value serializer type string.
    pub fn for_value_type(value_type: &str) -> Self {
        match value_type {
            "Byte" | "Short" | "Int" | "Long" => ValueColumn::Int64,
            "Float" | "Double" => ValueColumn::Float64,
            "Boolean" => ValueColumn::Boolean,
            // A `Char` decodes to a one-character string.
            "String" | "Char" => ValueColumn::Utf8,
            _ => ValueColumn::Json,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            ValueColumn::Int64 => DataType::Int64,
            ValueColumn::Float64 => DataType::Float64,
            ValueColumn::Boolean => DataType::Boolean,
            ValueColumn::Utf8 | ValueColumn::Json => DataType::Utf8,
        }
    }
}

fn schema(value: ValueColumn) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("operator", DataType::Utf8, false),
        Field::new("subtask", DataType::Int32, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("state_type", DataType::Utf8, false),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("key_group", DataType::Int32, false),
        Field::new("map_key", DataType::Utf8, true),
        Field::new("namespace_hex", DataType::Utf8, true),
        Field::new("value", value.data_type(), true),
        // Why `value` is NULL when the state did hold something: the decoder's marker (e.g.
        // `[undecodable: Long]`), or the JSON of a value the typed column cannot hold. NULL
        // otherwise, so a NULL `value` with a NULL note is a value Flink stored as null.
        Field::new("value_note", DataType::Utf8, true),
    ]))
}

enum ValueBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Str(StringBuilder),
}

struct Builders {
    operator: StringBuilder,
    subtask: Int32Builder,
    state: StringBuilder,
    state_type: StringBuilder,
    value_type: StringBuilder,
    key: StringBuilder,
    key_group: Int32Builder,
    map_key: StringBuilder,
    namespace_hex: StringBuilder,
    value: ValueBuilder,
    value_note: StringBuilder,
    rows: usize,
}

impl Builders {
    fn new(kind: ValueColumn) -> Self {
        Self {
            operator: StringBuilder::new(),
            subtask: Int32Builder::new(),
            state: StringBuilder::new(),
            state_type: StringBuilder::new(),
            value_type: StringBuilder::new(),
            key: StringBuilder::new(),
            key_group: Int32Builder::new(),
            map_key: StringBuilder::new(),
            namespace_hex: StringBuilder::new(),
            value: match kind {
                ValueColumn::Int64 => ValueBuilder::Int64(Int64Builder::new()),
                ValueColumn::Float64 => ValueBuilder::Float64(Float64Builder::new()),
                ValueColumn::Boolean => ValueBuilder::Boolean(BooleanBuilder::new()),
                ValueColumn::Utf8 | ValueColumn::Json => ValueBuilder::Str(StringBuilder::new()),
            },
            value_note: StringBuilder::new(),
            rows: 0,
        }
    }

    fn push(&mut self, row: &StateRow<'_>, kind: ValueColumn) {
        self.operator.append_value(row.operator);
        self.subtask.append_value(row.subtask);
        self.state.append_value(row.state);
        self.state_type.append_value(row.state_type);
        self.value_type.append_value(row.value_type);
        self.key.append_value(row.key);
        self.key_group.append_value(row.key_group as i32);
        self.map_key.append_option(row.map_key);
        self.namespace_hex.append_option(row.namespace_hex);

        // A value that does not fit the column — a `null`, or a number where the serializer said
        // boolean — becomes NULL rather than a silently coerced wrong value. So does, in a typed
        // scalar column, the marker of a value that could not be decoded (`parser::value`): only
        // bytes that end before the scalar does produce one there.
        let fits = match (&mut self.value, kind) {
            (ValueBuilder::Int64(b), _) => {
                b.append_option(row.value.as_i64());
                row.value.as_i64().is_some()
            }
            (ValueBuilder::Float64(b), _) => {
                b.append_option(row.value.as_f64());
                row.value.as_f64().is_some()
            }
            (ValueBuilder::Boolean(b), _) => {
                b.append_option(row.value.as_bool());
                row.value.as_bool().is_some()
            }
            (ValueBuilder::Str(b), ValueColumn::Utf8) => {
                b.append_option(row.value.as_str());
                row.value.as_str().is_some()
            }
            (ValueBuilder::Str(b), _) => {
                if row.value.is_null() {
                    b.append_null();
                } else {
                    b.append_value(row.value.to_string());
                }
                true
            }
        };
        if fits || row.value.is_null() {
            self.value_note.append_null();
        } else {
            match row.value.as_str() {
                Some(marker) => self.value_note.append_value(marker),
                None => self.value_note.append_value(row.value.to_string()),
            }
        }
        self.rows += 1;
    }

    fn finish(&mut self, schema: Arc<Schema>) -> Result<RecordBatch, String> {
        let value: ArrayRef = match &mut self.value {
            ValueBuilder::Int64(b) => Arc::new(b.finish()),
            ValueBuilder::Float64(b) => Arc::new(b.finish()),
            ValueBuilder::Boolean(b) => Arc::new(b.finish()),
            ValueBuilder::Str(b) => Arc::new(b.finish()),
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.operator.finish()),
            Arc::new(self.subtask.finish()),
            Arc::new(self.state.finish()),
            Arc::new(self.state_type.finish()),
            Arc::new(self.value_type.finish()),
            Arc::new(self.key.finish()),
            Arc::new(self.key_group.finish()),
            Arc::new(self.map_key.finish()),
            Arc::new(self.namespace_hex.finish()),
            value,
            Arc::new(self.value_note.finish()),
        ];
        self.rows = 0;
        RecordBatch::try_new(schema, columns).map_err(|e| format!("arrow batch: {}", e))
    }
}

/// Writes exported rows as a single Parquet file, zstd-compressed.
pub struct ParquetSink<W: std::io::Write + Send> {
    /// Taken when the first row arrives: the schema depends on that row's `value_type`.
    pending: Option<W>,
    writer: Option<ArrowWriter<W>>,
    schema: Option<Arc<Schema>>,
    kind: ValueColumn,
    builders: Option<Builders>,
}

impl ParquetSink<std::fs::File> {
    pub fn create(path: &Path) -> Result<Self, String> {
        let file =
            std::fs::File::create(path).map_err(|e| format!("create {}: {}", path.display(), e))?;
        Ok(Self::new(file))
    }
}

impl<W: std::io::Write + Send> ParquetSink<W> {
    pub fn new(out: W) -> Self {
        Self {
            pending: Some(out),
            writer: None,
            schema: None,
            kind: ValueColumn::Json,
            builders: None,
        }
    }

    /// The column type chosen for `value`, once a row has been seen.
    pub fn value_column(&self) -> ValueColumn {
        self.kind
    }

    fn properties() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(3).expect("zstd level 3 is valid"),
            ))
            .set_max_row_group_row_count(Some(BATCH_ROWS))
            .build()
    }

    fn start(&mut self, value_type: &str) -> Result<(), String> {
        let kind = ValueColumn::for_value_type(value_type);
        let schema = schema(kind);
        let out = self
            .pending
            .take()
            .ok_or_else(|| "parquet writer already started".to_string())?;
        let writer = ArrowWriter::try_new(out, schema.clone(), Some(Self::properties()))
            .map_err(|e| format!("parquet writer: {}", e))?;
        self.kind = kind;
        self.schema = Some(schema);
        self.writer = Some(writer);
        self.builders = Some(Builders::new(kind));
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<(), String> {
        let (Some(builders), Some(schema), Some(writer)) = (
            self.builders.as_mut(),
            self.schema.clone(),
            self.writer.as_mut(),
        ) else {
            return Ok(());
        };
        if builders.rows == 0 {
            return Ok(());
        }
        let batch = builders.finish(schema)?;
        writer
            .write(&batch)
            .map_err(|e| format!("parquet write: {}", e))
    }
}

impl<W: std::io::Write + Send> RowSink for ParquetSink<W> {
    fn row(&mut self, row: &StateRow<'_>) -> Result<(), String> {
        if self.writer.is_none() {
            self.start(row.value_type)?;
        }
        let kind = self.kind;
        if let Some(b) = self.builders.as_mut() {
            b.push(row, kind);
            if b.rows >= BATCH_ROWS {
                self.flush_batch()?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), String> {
        // A state with no entries still produces a valid, empty, readable file rather than none.
        if self.writer.is_none() {
            self.start("")?;
        }
        self.flush_batch()?;
        if let Some(w) = self.writer.take() {
            w.close().map_err(|e| format!("parquet close: {}", e))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, StringArray};
    use serde_json::{json, Value};

    fn row<'a>(value: &'a Value) -> StateRow<'a> {
        StateRow {
            operator: "op",
            subtask: 0,
            state: "s",
            state_type: "VALUE",
            value_type: "Long",
            key: "k",
            key_group: 0,
            map_key: None,
            namespace_hex: None,
            value,
        }
    }

    #[test]
    fn a_null_typed_value_says_why_unless_flink_stored_null() {
        let mut b = Builders::new(ValueColumn::Int64);
        let (ok, null, marker) = (json!(7), Value::Null, json!("[undecodable: Long]"));
        for v in [&ok, &null, &marker] {
            b.push(&row(v), ValueColumn::Int64);
        }
        let batch = b.finish(schema(ValueColumn::Int64)).unwrap();
        let value = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let note = batch
            .column_by_name("value_note")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value.value(0), 7);
        assert!(note.is_null(0));
        assert!(
            value.is_null(1) && note.is_null(1),
            "a stored null has no note"
        );
        assert!(value.is_null(2));
        assert_eq!(note.value(2), "[undecodable: Long]");
    }
}
