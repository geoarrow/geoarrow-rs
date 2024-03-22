use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::file::{BinaryFileReader, BinaryFileWriter};
use crate::io::object_store::PyObjectStore;
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use geoarrow::io::flatgeobuf::{read_flatgeobuf as _read_flatgeobuf, FlatGeobufReaderOptions};
use pyo3::prelude::*;

/// Read a FlatGeobuf file from a path on disk into a GeoTable.
///
/// Example:
///
/// Reading a remote file on an S3 bucket.
///
/// ```py
/// from geoarrow.rust.core import ObjectStore, read_flatgeobuf_async
///
/// options = {
///     "aws_access_key_id": "...",
///     "aws_secret_access_key": "...",
///     "aws_region": "..."
/// }
/// fs = ObjectStore('s3://bucket', options=options)
/// table = read_flatgeobuf("path/in/bucket.fgb", fs)
/// ```
///
/// Args:
///     file: the path to the file or a Python file object in binary read mode.
///
/// Other args:
///     fs: an ObjectStore instance for this url. This is required only if the file is at a remote
///         location.
///     batch_size: the number of rows to include in each internal batch of the table.
///     bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy). If set to
///       `None`, no spatial filtering will be performed.
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
#[pyo3(signature = (file, *, fs=None, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf(
    py: Python,
    file: PyObject,
    fs: Option<PyObjectStore>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<GeoTable> {
    if let Some(fs) = fs {
        fs.rt.block_on(async move {
            let options = FlatGeobufReaderOptions {
                batch_size: Some(batch_size),
                bbox,
                ..Default::default()
            };
            let path = file.extract::<String>(py)?;
            let table = _read_flatgeobuf_async(fs.inner, path.into(), options)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;

            Ok(GeoTable(table))
        })
    } else {
        let mut reader = file.extract::<BinaryFileReader>(py)?;
        let options = FlatGeobufReaderOptions {
            batch_size: Some(batch_size),
            bbox,
            ..Default::default()
        };
        let table = _read_flatgeobuf(&mut reader, options)?;
        Ok(GeoTable(table))
    }
}

/// Read a FlatGeobuf file from a url into a GeoTable.
///
/// Example:
///
///     ```py
///     from geoarrow.rust.core import ObjectStore, read_flatgeobuf_async
///
///     options = {
///         "aws_access_key_id": "...",
///         "aws_secret_access_key": "...",
///     }
///     fs = ObjectStore('s3://bucket', options=options)
///     table = await read_flatgeobuf_async("path/in/bucket.fgb", fs)
///     ```
///
/// Args:
///     url: the url to a remote FlatGeobuf file
///     fs: an ObjectStore instance for this url.
///
/// Other args:
///     batch_size: the number of rows to include in each internal batch of the table.
///     bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy). If set to
///       `None`, no spatial filtering will be performed.
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
#[pyo3(signature = (path, fs, *, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf_async(
    py: Python,
    path: String,
    fs: PyObjectStore,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<PyObject> {
    let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
        let options = FlatGeobufReaderOptions {
            batch_size: Some(batch_size),
            bbox,
            ..Default::default()
        };
        let table = _read_flatgeobuf_async(fs.inner, path.into(), options)
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
