use std::fs::File;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Field, Schema};
use bytes::Bytes;

use crate::array::GeometryBuilder;
use crate::chunked_array::ChunkedNativeArrayDyn;
use crate::table::Table;
use crate::{GeoParquetRecordBatchReaderBuilder, write_geoparquet};
use geoarrow_array::error::Result;

#[ignore = "fails!"]
#[test]
fn round_trip_nybb() -> Result<()> {
    let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
    let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let schema = reader.schema();

    let mut buf = vec![];
    write_geoparquet(Box::new(reader), Cursor::new(&mut buf), &Default::default()).unwrap();
    let again_reader = GeoParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf))?.build()?;

    assert_eq!(schema.as_ref(), again_reader.schema().as_ref());
    Ok(())
    // assert_eq!(table.geometry().unwrap().ch, again.geometry().unwrap());
}

// Test from https://github.com/geoarrow/geoarrow-rs/pull/717
#[ignore = "Union fields length must match child arrays length"]
#[test]
fn mixed_geometry_roundtrip() {
    let mut builder = GeometryBuilder::new();
    builder
        .push_point(Some(&geo_types::point!(x: -105., y: 40.)))
        .unwrap();
    let geometry = ChunkedNativeArrayDyn::from_geoarrow_chunks(&[&builder.finish()])
        .unwrap()
        .into_inner();
    let array = BooleanArray::from(vec![true]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "visible",
        arrow_schema::DataType::Boolean,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let table = Table::from_arrow_and_geometry(vec![batch], schema, geometry).unwrap();
    let mut cursor = Cursor::new(Vec::new());
    write_geoparquet(
        table.into_record_batch_reader(),
        &mut cursor,
        &Default::default(),
    )
    .unwrap();
    let bytes = Bytes::from(cursor.into_inner());
    GeoParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .read_table()
        .unwrap();
}
