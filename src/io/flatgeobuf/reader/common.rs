use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use flatgeobuf::{ColumnType, Header};

use crate::array::CoordType;

/// Options for the FlatGeobuf reader
#[derive(Debug, Clone, Copy)]
pub struct FlatGeobufReaderOptions {
    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// The number of rows in each batch.
    pub batch_size: Option<usize>,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    pub bbox: Option<(f64, f64, f64, f64)>,
}

impl Default for FlatGeobufReaderOptions {
    fn default() -> Self {
        Self {
            coord_type: Default::default(),
            batch_size: Some(65_536),
            bbox: None,
        }
    }
}

pub(super) fn infer_schema(header: Header<'_>) -> SchemaRef {
    let columns = header.columns().unwrap();
    let mut schema = SchemaBuilder::with_capacity(columns.len());

    for col in columns.into_iter() {
        let field = match col.type_() {
            ColumnType::Bool => Field::new(col.name(), DataType::Boolean, col.nullable()),
            ColumnType::Byte => Field::new(col.name(), DataType::Int8, col.nullable()),
            ColumnType::UByte => Field::new(col.name(), DataType::UInt8, col.nullable()),
            ColumnType::Short => Field::new(col.name(), DataType::Int16, col.nullable()),
            ColumnType::UShort => Field::new(col.name(), DataType::UInt16, col.nullable()),
            ColumnType::Int => Field::new(col.name(), DataType::Int32, col.nullable()),
            ColumnType::UInt => Field::new(col.name(), DataType::UInt32, col.nullable()),
            ColumnType::Long => Field::new(col.name(), DataType::Int64, col.nullable()),
            ColumnType::ULong => Field::new(col.name(), DataType::UInt64, col.nullable()),
            ColumnType::Float => Field::new(col.name(), DataType::Float32, col.nullable()),
            ColumnType::Double => Field::new(col.name(), DataType::Float64, col.nullable()),
            ColumnType::String => Field::new(col.name(), DataType::Utf8, col.nullable()),
            ColumnType::Json => {
                let mut metadata = HashMap::with_capacity(1);
                metadata.insert("ARROW:extension:name".to_string(), "arrow.json".to_string());
                Field::new(col.name(), DataType::Utf8, col.nullable()).with_metadata(metadata)
            }
            ColumnType::DateTime => Field::new(
                col.name(),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                col.nullable(),
            ),
            ColumnType::Binary => Field::new(col.name(), DataType::Binary, col.nullable()),
            // ColumnType is actually a struct, not an enum, so the rust compiler doesn't know
            // we've matched all types
            _ => unreachable!(),
        };
        schema.push(field);
    }

    Arc::new(schema.finish())
}
