use std::fs::File;
use std::io::BufReader;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::geojson::read_geojson as _read_geojson;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

#[pyfunction]
pub fn read_geojson(path: String) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let table = _read_geojson(&mut reader)?;
    Ok(GeoTable(table))
}
