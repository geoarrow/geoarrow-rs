use std::collections::HashMap;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::file::{BinaryFileReader, BinaryFileWriter};
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use geoarrow::io::flatgeobuf::{read_flatgeobuf as _read_flatgeobuf, FlatGeobufReaderOptions};
use object_store::{parse_url, parse_url_opts};
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
#[pyo3(signature = (file, *, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf(
    py: Python,
    file: PyObject,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<GeoTable> {
    let mut reader = file.extract::<BinaryFileReader>(py)?;
    let options = FlatGeobufReaderOptions {
        batch_size: Some(batch_size),
        bbox,
        ..Default::default()
    };
    let table = _read_flatgeobuf(&mut reader, options)?;
    Ok(GeoTable(table))
}

/// Read a FlatGeobuf file from a url into a GeoTable.
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
#[pyo3(signature = (url, *, batch_size=65536, options=None, bbox=None))]
pub fn read_flatgeobuf_async(
    py: Python,
    url: String,
    batch_size: usize,
    options: Option<HashMap<String, String>>,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<PyObject> {
    let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
        let url = Url::parse(&url).map_err(|err| PyValueError::new_err(err.to_string()))?;
        let (reader, location) = if let Some(options) = options {
            parse_url_opts(&url, options)
        } else {
            parse_url(&url)
        }
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
        // dbg!(&reader);
        // dbg!(&location);

        let options = FlatGeobufReaderOptions {
            batch_size: Some(batch_size),
            bbox,
            ..Default::default()
        };
        let table = _read_flatgeobuf_async(reader, location, options)
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
