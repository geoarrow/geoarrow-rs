use crate::error::PyGeoArrowResult;
use crate::io::file::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use pyo3::prelude::*;

/// Read a FlatGeobuf file from a path on disk into a GeoTable.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///     batch_size: the number of rows to include in each internal batch of the table.
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
#[pyo3(signature = (file, *, batch_size=65536))]
pub fn read_flatgeobuf(
    py: Python,
    file: PyObject,
    batch_size: usize,
) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_flatgeobuf(&mut reader, Default::default(), Some(batch_size))?;
    Ok(GeoTable(table))
}

/// Write a GeoTable to a FlatGeobuf file on disk.
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
#[pyo3(signature = (table, file, *, write_index=true))]
pub fn write_flatgeobuf(
    py: Python,
    table: &PyAny,
    file: PyObject,
    write_index: bool,
) -> PyGeoArrowResult<()> {
    let mut table: GeoTable = FromPyObject::extract(table)?;
    let writer = file.extract::<BinaryFileWriter>(py)?;
    let name = writer.file_stem(py);

    let options = FgbWriterOptions {
        write_index,
        ..Default::default()
    };
    _write_flatgeobuf(&mut table.0, writer, name.as_deref().unwrap_or(""), options)?;
    Ok(())
}
