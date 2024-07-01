use crate::error::PyGeoArrowResult;
use crate::interop::util::table_to_pytable;
use crate::io::input::sync::{BinaryFileReader, BinaryFileWriter};
use geoarrow::io::ipc::read_ipc as _read_ipc;
use geoarrow::io::ipc::read_ipc_stream as _read_ipc_stream;
use geoarrow::io::ipc::write_ipc as _write_ipc;
use geoarrow::io::ipc::write_ipc_stream as _write_ipc_stream;
use pyo3::prelude::*;
use pyo3_arrow::{PyRecordBatchReader, PyTable};

/// Read into a Table from Arrow IPC (Feather v2) file.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///
/// Returns:
///     Table from Arrow IPC file.
#[pyfunction]
#[pyo3(signature = (file))]
pub fn read_ipc(py: Python, file: PyObject) -> PyGeoArrowResult<PyTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_ipc(&mut reader)?;
    Ok(table_to_pytable(table))
}

/// Read into a Table from Arrow IPC record batch stream.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///
/// Returns:
///     Table from Arrow IPC file.
#[pyfunction]
#[pyo3(signature = (file))]
pub fn read_ipc_stream(py: Python, file: PyObject) -> PyGeoArrowResult<PyTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_ipc_stream(&mut reader)?;
    Ok(table_to_pytable(table))
}

/// Write a GeoTable to an Arrow IPC (Feather v2) file on disk.
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
pub fn write_ipc(py: Python, table: PyRecordBatchReader, file: PyObject) -> PyGeoArrowResult<()> {
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_ipc(table.into_reader()?, writer)?;
    Ok(())
}

/// Write a GeoTable to an Arrow IPC stream
///
/// Args:
///     table: the table to write.
///     file: the path to the file or a Python file object in binary write mode.
///
/// Returns:
///     None
#[pyfunction]
pub fn write_ipc_stream(
    py: Python,
    table: PyRecordBatchReader,
    file: PyObject,
) -> PyGeoArrowResult<()> {
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_ipc_stream(table.into_reader()?, writer)?;
    Ok(())
}
