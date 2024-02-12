use crate::error::PyGeoArrowResult;
use crate::io::file::BinaryFileReader;
use crate::table::GeoTable;
use geoarrow::io::geojson_lines::read_geojson_lines as _read_geojson_lines;
use pyo3::prelude::*;

/// Read a GeoJSON Lines file from a path on disk into a GeoTable.
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
