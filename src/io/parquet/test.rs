use std::fs::File;
use std::io::Cursor;

use bytes::Bytes;

use crate::io::parquet::{read_geoparquet, write_geoparquet};

#[ignore = "fails!"]
#[test]
fn round_trip_nybb() {
    let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
    let mut table = read_geoparquet(file, Default::default()).unwrap();

    let mut buf = vec![];
    write_geoparquet(&mut table, Cursor::new(&mut buf), &Default::default()).unwrap();
    let again = read_geoparquet(Bytes::from(buf), Default::default()).unwrap();
    assert_eq!(table.schema(), again.schema());
    // assert_eq!(table.geometry().unwrap().ch, again.geometry().unwrap());
}
