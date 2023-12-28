use std::fs::File;
use std::io::BufReader;

use crate::table::GeoTable;
use geoarrow::io::geojson::read_geojson as _read_geojson;
use pyo3::prelude::*;

#[pyfunction]
pub fn read_geojson(path: String) -> GeoTable {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(f);
    let table = _read_geojson(&mut reader).unwrap();
    GeoTable(table)
}
