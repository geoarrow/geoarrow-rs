use crate::error::PyGeoArrowResult;
use crate::io::input::sync::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use geoarrow::io::geojson_lines::read_geojson_lines as _read_geojson_lines;
use geoarrow::io::geojson_lines::write_geojson_lines as _write_geojson_lines;
use pyo3::prelude::*;

/// Read a newline-delimited GeoJSON file from a path on disk into a GeoTable.
///
/// This expects a GeoJSON Feature on each line of a text file, with a newline character separating
/// each Feature.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///
/// Returns:
///     Table from GeoJSON file.
#[pyfunction]
#[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_geojson_lines(
    py: Python,
    file: PyObject,
    batch_size: usize,
) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_geojson_lines(&mut reader, Some(batch_size))?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a newline-delimited GeoJSON file on disk.
///
/// Note that the GeoJSON specification mandates coordinates to be in the WGS84 (EPSG:4326)
/// coordinate system, but this function will not automatically reproject into WGS84 for you.
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
pub fn write_geojson_lines(
    py: Python,
    mut table: GeoTable,
    file: PyObject,
) -> PyGeoArrowResult<()> {
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_geojson_lines(&mut table.0, writer)?;
    Ok(())
}
