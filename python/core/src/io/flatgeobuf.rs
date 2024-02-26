use std::collections::HashMap;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::file::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use object_store::{parse_url, parse_url_opts};
use object_store_python::{PyObjectStore, PyPath};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use url::Url;

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

/// Read a FlatGeobuf file from a url into a GeoTable.
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
#[pyo3(signature = (path, fs, *, batch_size=65536))]
pub fn read_flatgeobuf_async(
    py: Python,
    path: String,
    fs: PyObjectStore,
    batch_size: usize,
) -> PyGeoArrowResult<PyObject> {
    let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
        let table = _read_flatgeobuf_async(
            fs.inner,
            path.into(),
            Default::default(),
            Some(batch_size),
            None,
        )
        .await
        .map_err(PyGeoArrowError::GeoArrowError)?;

        Ok(GeoTable(table))
    })?;
    Ok(fut.into())
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
    mut table: GeoTable,
    file: PyObject,
    write_index: bool,
) -> PyGeoArrowResult<()> {
    let writer = file.extract::<BinaryFileWriter>(py)?;
    let name = writer.file_stem(py);

    let options = FgbWriterOptions {
        write_index,
        ..Default::default()
    };
    _write_flatgeobuf(&mut table.0, writer, name.as_deref().unwrap_or(""), options)?;
    Ok(())
}
