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

/// Build [ArrowReaderMetadata] with the given footer row count and per-row-group row counts.
pub(crate) fn arrow_meta(
    message_type: &str,
    num_rows: i64,
    row_group_counts: &[i64],
) -> ArrowReaderMetadata {
    let descr = schema_descr(message_type);
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
