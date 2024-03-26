use crate::error::PyGeoArrowResult;
use crate::io::input::sync::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use geoarrow::io::ipc::read_ipc as _read_ipc;
use geoarrow::io::ipc::read_ipc_stream as _read_ipc_stream;
use geoarrow::io::ipc::write_ipc as _write_ipc;
use geoarrow::io::ipc::write_ipc_stream as _write_ipc_stream;
use pyo3::prelude::*;

/// Read into a Table from Arrow IPC (Feather v2) file.
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///
/// Returns:
///     Table from Arrow IPC file.
#[pyfunction]
#[pyo3(signature = (file))]
pub fn read_ipc(py: Python, file: PyObject) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_ipc(&mut reader)?;
    Ok(GeoTable(table))
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
pub fn read_ipc_stream(py: Python, file: PyObject) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let table = _read_ipc_stream(&mut reader)?;
    Ok(GeoTable(table))
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
pub fn write_ipc(py: Python, mut table: GeoTable, file: PyObject) -> PyGeoArrowResult<()> {
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_ipc(&mut table.0, writer)?;
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
pub fn write_ipc_stream(py: Python, mut table: GeoTable, file: PyObject) -> PyGeoArrowResult<()> {
    let writer = file.extract::<BinaryFileWriter>(py)?;
    _write_ipc_stream(&mut table.0, writer)?;
    Ok(())
}
