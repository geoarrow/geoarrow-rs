use crate::error::PyGeoArrowResult;
use crate::io::file::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use geoarrow::io::geojson::read_geojson as _read_geojson;
use geoarrow::io::geojson::write_geojson as _write_geojson;
use pyo3::prelude::*;

/// Read a GeoJSON file from a path on disk into a GeoTable.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from GeoJSON file.
#[pyfunction]
#[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_geojson(py: Python, file: PyObject, batch_size: usize) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_geojson(&mut reader, Some(batch_size))?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a GeoJSON file on disk.
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
pub fn write_geojson(py: Python, table: &PyAny, file: PyObject) -> PyGeoArrowResult<()> {
    let mut table: GeoTable = FromPyObject::extract(table)?;
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_geojson(&mut table.0, writer)?;
    Ok(())
}
