use std::fs::File;
use std::io::{BufReader, BufWriter};

use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::io::geojson::read_geojson as _read_geojson;
use geoarrow::io::geojson::write_geojson as _write_geojson;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;

/// Read a GeoJSON file from a path on disk into a GeoTable.
///
/// Args:
///     path: the path to the file
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from GeoJSON file.
#[pyfunction]
pub fn read_geojson(path: String, batch_size: Option<usize>) -> PyGeoArrowResult<GeoTable> {
    let f = File::open(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let mut reader = BufReader::new(f);
    let table = _read_geojson(&mut reader, batch_size)?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a GeoJSON file on disk.
///
/// Note that the GeoJSON specification mandates coordinates to be in the WGS84 (EPSG:4326)
/// coordinate system, but this function will not automatically reproject into WGS84 for you.
///
/// Args:
///     table: the table to write.
///     path: the path to the file.
///
/// Returns:
///     None
#[pyfunction]
pub fn write_geojson(table: &PyAny, path: String) -> PyGeoArrowResult<()> {
    let mut table: GeoTable = FromPyObject::extract(table)?;
    let f = File::create(path).map_err(|err| PyFileNotFoundError::new_err(err.to_string()))?;
    let writer = BufWriter::new(f);
    _write_geojson(&mut table.0, writer)?;
    Ok(())
}
