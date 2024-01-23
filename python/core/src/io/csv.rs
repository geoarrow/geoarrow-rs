use crate::error::PyGeoArrowResult;
use crate::io::file::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use geoarrow::io::csv::read_csv as _read_csv;
use geoarrow::io::csv::write_csv as _write_csv;
use geoarrow::io::csv::CSVReaderOptions;
use pyo3::prelude::*;

/// Read a CSV file from a path on disk into a GeoTable.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///     geometry_column_name: the name of the geometry column within the CSV.
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from CSV file.
#[pyfunction]
#[pyo3(signature = (file, geometry_column_name, *, batch_size=65536))]
pub fn read_csv(
    py: Python,
    file: PyObject,
    geometry_column_name: &str,
    batch_size: usize,
) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let options = CSVReaderOptions::new(Default::default(), batch_size);
    let table = _read_csv(&mut reader, geometry_column_name, options)?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a CSV file on disk.
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
#[pyo3(signature = (table, file))]
pub fn write_csv(py: Python, table: &PyAny, file: PyObject) -> PyGeoArrowResult<()> {
    let mut table: GeoTable = FromPyObject::extract(table)?;
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_csv(&mut table.0, writer)?;
    Ok(())
}
