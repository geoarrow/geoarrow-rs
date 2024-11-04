use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::{construct_reader, AnyFileReader};
use crate::util::table_to_pytable;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow::io::flatgeobuf::FlatGeobufReaderOptions;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

#[pyfunction]
#[pyo3(signature = (path, *, fs=None, batch_size=65536, bbox=None))]
pub fn read_flatgeobuf_async(
    py: Python,
    path: PyObject,
    fs: Option<PyObject>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_reader(py, path, fs)?;
    match reader {
        AnyFileReader::Async(async_reader) => {
            let fut = future_into_py(py, async move {
                let options = FlatGeobufReaderOptions {
                    batch_size: Some(batch_size),
                    bbox,
                    ..Default::default()
                };
                let table = _read_flatgeobuf_async(async_reader.store, async_reader.path, options)
                    .await
                    .map_err(PyGeoArrowError::GeoArrowError)?;

                Ok(table_to_pytable(table))
            })?;
            Ok(fut.into())
        }
        AnyFileReader::Sync(_) => {
            Err(PyValueError::new_err("Local file paths not supported in async reader.").into())
        }
    }
}
