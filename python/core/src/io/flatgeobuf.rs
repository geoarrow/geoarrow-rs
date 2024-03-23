use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::sync::BinaryFileWriter;
use crate::io::input::{construct_reader, FileReader};
use crate::io::object_store::PyObjectStore;
use crate::table::GeoTable;
use flatgeobuf::FgbWriterOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow::io::flatgeobuf::write_flatgeobuf_with_options as _write_flatgeobuf;
use geoarrow::io::flatgeobuf::{read_flatgeobuf as _read_flatgeobuf, FlatGeobufReaderOptions};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Read a FlatGeobuf file from a path on disk or a remote location into a GeoTable.
///
/// Example:
///
/// Reading from a local path:
///
/// ```py
/// from geoarrow.rust.core import read_flatgeobuf
/// table = read_flatgeobuf("path/to/file.fgb")
/// ```
///
/// Reading from a Python file object:
///
/// ```py
/// from geoarrow.rust.core import read_flatgeobuf
///
/// with open("path/to/file.fgb", "rb") as file:
///     table = read_flatgeobuf(file)
/// ```
///
/// Reading from an HTTP(S) url:
///
/// ```py
/// from geoarrow.rust.core import read_flatgeobuf
///
/// url = "http://flatgeobuf.org/test/data/UScounties.fgb"
/// table = read_flatgeobuf(url)
/// ```
///
/// Reading from a remote file on an S3 bucket.
///
/// ```py
/// from geoarrow.rust.core import ObjectStore, read_flatgeobuf
///
/// options = {
///     "aws_access_key_id": "...",
///     "aws_secret_access_key": "...",
///     "aws_region": "..."
/// }
/// fs = ObjectStore('s3://bucket', options=options)
/// table = read_flatgeobuf("path/in/bucket.fgb", fs=fs)
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
    let reader = construct_reader(py, file, fs)?;
    match reader {
        FileReader::Async(async_reader) => async_reader.runtime.block_on(async move {
            let options = FlatGeobufReaderOptions {
                batch_size: Some(batch_size),
                bbox,
                ..Default::default()
            };
            let table = _read_flatgeobuf_async(async_reader.store, async_reader.path, options)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;

            Ok(GeoTable(table))
        }),
        FileReader::Sync(mut sync_reader) => {
            let options = FlatGeobufReaderOptions {
                batch_size: Some(batch_size),
                bbox,
                ..Default::default()
            };
            let table = _read_flatgeobuf(&mut sync_reader, options)?;
            Ok(GeoTable(table))
        }
    }
}

/// Read a FlatGeobuf file from a url into a GeoTable.
///
/// Example:
///
/// Reading from an HTTP(S) url:
///
/// ```py
/// from geoarrow.rust.core import read_flatgeobuf_async
///
/// url = "http://flatgeobuf.org/test/data/UScounties.fgb"
/// table = await read_flatgeobuf_async(url)
/// ```
///
/// Reading from an S3 bucket:
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
/// table = await read_flatgeobuf_async("path/in/bucket.fgb", fs=fs)
/// ```
///
/// Args:
///     path: the url or relative path to a remote FlatGeobuf file. If an argument is passed for
///         `fs`, this should be a path fragment relative to the root passed to the `ObjectStore`
///         constructor.
///
/// Other args:
///     fs: an ObjectStore instance for this url. This is required for non-HTTP urls.
///     batch_size: the number of rows to include in each internal batch of the table.
///     bbox: A spatial filter for reading rows, of the format (minx, miny, maxx, maxy). If set to
///       `None`, no spatial filtering will be performed.
///
/// Returns:
///     Table from FlatGeobuf file.
#[pyfunction]
#[pyo3(signature = (path, *, fs=None, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf_async(
    py: Python,
    path: PyObject,
    fs: Option<PyObjectStore>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(py, path, fs)?;
    match reader {
        FileReader::Async(async_reader) => {
            let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
                let options = FlatGeobufReaderOptions {
                    batch_size: Some(batch_size),
                    bbox,
                    ..Default::default()
                };
                let table = _read_flatgeobuf_async(async_reader.store, async_reader.path, options)
                    .await
                    .map_err(PyGeoArrowError::GeoArrowError)?;

                Ok(GeoTable(table))
            })?;
            Ok(fut.into())
        }
        FileReader::Sync(_) => {
            Err(PyValueError::new_err("Local file paths not supported in async reader.").into())
        }
    }
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
