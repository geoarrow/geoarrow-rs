use std::fs::File;
use std::io::BufReader;

use crate::table::GeoTable;
use geoarrow::io::csv::read_csv as _read_csv;
use pyo3::prelude::*;

#[pyfunction]
pub fn read_csv(path: String, geometry_column_name: &str) -> GeoTable {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(f);
    let table = _read_csv(&mut reader, geometry_column_name).unwrap();
    GeoTable(table)
}
