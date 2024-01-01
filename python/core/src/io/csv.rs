use std::fs::File;
use std::io::BufReader;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::csv::read_csv as _read_csv;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

#[pyfunction]
pub fn read_csv(path: String, geometry_column_name: &str) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let table = _read_csv(&mut reader, geometry_column_name)?;
    Ok(GeoTable(table))
}
