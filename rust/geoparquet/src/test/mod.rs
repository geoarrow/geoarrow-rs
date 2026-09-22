mod geoarrow_data;

use std::path::PathBuf;

pub(crate) fn fixture_dir() -> PathBuf {
    let p = PathBuf::from("../../fixtures");
    assert!(p.exists());
    p
}

pub(crate) fn geoarrow_data_example_files() -> PathBuf {
    fixture_dir().join("geoarrow-data/example/files")
}

pub(crate) fn geoarrow_data_example_crs_files() -> PathBuf {
    fixture_dir().join("geoarrow-data/example-crs/files")
}

use std::sync::Arc;

use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescriptor;

pub(crate) fn schema_descr(message_type: &str) -> Arc<SchemaDescriptor> {
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    Arc::new(SchemaDescriptor::new(schema))
}

/// SchemaDescriptor with one top-level BYTE_ARRAY `geometry` column that carries the given
/// logical type, next to a plain string column.
pub(crate) fn geometry_schema_descr(
    logical_type: parquet::basic::LogicalType,
) -> Arc<SchemaDescriptor> {
    use parquet::basic::{LogicalType, Type as PhysicalType};
    use parquet::schema::types::Type;

    let geometry = Type::primitive_type_builder("geometry", PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(logical_type))
        .build()
        .unwrap();
    let name = Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(LogicalType::String))
        .build()
        .unwrap();
    let root = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(geometry), Arc::new(name)])
        .build()
        .unwrap();
    Arc::new(SchemaDescriptor::new(Arc::new(root)))
}

/// Build [ArrowReaderMetadata] from a schema descriptor with the given footer row count and
/// per-row-group row counts.
pub(crate) fn arrow_meta_from_descr(
    descr: Arc<SchemaDescriptor>,
    num_rows: i64,
    row_group_counts: &[i64],
) -> ArrowReaderMetadata {
    let row_groups = row_group_counts
        .iter()
        .map(|count| {
            let column = ColumnChunkMetaData::builder(descr.column(0))
                .build()
                .unwrap();
            RowGroupMetaData::builder(descr.clone())
                .set_num_rows(*count)
                .set_column_metadata(vec![column])
                .build()
                .unwrap()
        })
        .collect();
    let file_meta = FileMetaData::new(1, num_rows, None, None, descr, None);
    let parquet_meta = ParquetMetaData::new(file_meta, row_groups);
    ArrowReaderMetadata::try_new(Arc::new(parquet_meta), Default::default()).unwrap()
}

/// Build [ArrowReaderMetadata] with the given footer row count and per-row-group row counts.
pub(crate) fn arrow_meta(
    message_type: &str,
    num_rows: i64,
    row_group_counts: &[i64],
) -> ArrowReaderMetadata {
    arrow_meta_from_descr(schema_descr(message_type), num_rows, row_group_counts)
}
