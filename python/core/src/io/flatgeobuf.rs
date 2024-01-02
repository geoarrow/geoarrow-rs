use std::fs::File;
use std::io::BufReader;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a FlatGeobuf file from a path on disk into a GeoTable.
///
/// Args:
///     path: the path to the file
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
pub fn read_flatgeobuf(path: String) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let table = _read_flatgeobuf(&mut reader)?;
    Ok(GeoTable(table))
}
