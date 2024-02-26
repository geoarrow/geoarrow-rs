use arrow_schema::{DataType, Field, SchemaBuilder, TimeUnit};
use flatgeobuf::{ColumnType, Header};

pub(super) fn infer_schema(header: Header<'_>) -> SchemaBuilder {
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
            ColumnType::Json => Field::new(col.name(), DataType::Utf8, col.nullable()),
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

    schema
}
