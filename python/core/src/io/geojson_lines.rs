use std::fs::File;
use std::io::BufReader;

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::geojson_lines::read_geojson_lines as _read_geojson_lines;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a GeoJSON Lines file from a path on disk into a GeoTable.
///
/// This expects a GeoJSON Feature on each line of a text file, with a newline character separating
/// each Feature.
///
/// Args:
///     path: the path to the file
///
/// Returns:
///     Table from GeoJSON file.
#[pyfunction]
pub fn read_geojson_lines(path: String, batch_size: Option<usize>) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let table = _read_geojson_lines(&mut reader, batch_size)?;
    Ok(GeoTable(table))
}
