use std::fs::File;
use std::io::BufReader;

use crate::table::GeoTable;
use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use pyo3::prelude::*;

#[pyfunction]
pub fn read_flatgeobuf(path: String) -> GeoTable {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(f);
    let table = _read_flatgeobuf(&mut reader).unwrap();
    GeoTable(table)
}
