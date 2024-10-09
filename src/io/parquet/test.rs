use std::fs::File;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema};
use bytes::Bytes;

use crate::array::MixedGeometryBuilder;
use crate::chunked_array::ChunkedNativeArrayDyn;
use crate::error::Result;
use crate::io::parquet::{write_geoparquet, GeoParquetRecordBatchReaderBuilder};
use crate::table::Table;

#[ignore = "fails!"]
#[test]
fn round_trip_nybb() -> Result<()> {
    let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
    let table = GeoParquetRecordBatchReaderBuilder::try_new(file)?
        .build()?
        .read_table()?;

    let schema = table.schema().clone();

    let mut buf = vec![];
    write_geoparquet(
        table.into_record_batch_reader(),
        Cursor::new(&mut buf),
        &Default::default(),
    )
    .unwrap();
    let again = GeoParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf))?
        .build()?
        .read_table()?;

    assert_eq!(&schema, again.schema());
    Ok(())
    // assert_eq!(table.geometry().unwrap().ch, again.geometry().unwrap());
}

// Test from https://github.com/geoarrow/geoarrow-rs/pull/717
#[test]
fn mixed_geometry_roundtrip() {
    let mut builder = MixedGeometryBuilder::<2>::new();
    builder
        .push_point(Some(&geo::point!(x: -105., y: 40.)))
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
