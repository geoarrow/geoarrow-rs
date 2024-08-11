use std::fs::File;
use std::io::Cursor;

use bytes::Bytes;

use crate::error::Result;
use crate::io::parquet::{write_geoparquet, GeoParquetRecordBatchReaderBuilder};

#[ignore = "fails!"]
#[test]
fn round_trip_nybb() -> Result<()> {
    let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
    let mut table = GeoParquetRecordBatchReaderBuilder::try_new(file)?
        .build()?
        .read_table()?;

    let mut buf = vec![];
    write_geoparquet(&mut table, Cursor::new(&mut buf), &Default::default()).unwrap();
    let again = GeoParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf))?
        .build()?
        .read_table()?;

    assert_eq!(table.schema(), again.schema());
    Ok(())
    // assert_eq!(table.geometry().unwrap().ch, again.geometry().unwrap());
}
