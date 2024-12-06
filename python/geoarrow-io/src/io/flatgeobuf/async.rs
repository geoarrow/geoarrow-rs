use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::io::input::construct_async_reader;
use crate::util::table_to_pytable;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow::io::flatgeobuf::FlatGeobufReaderOptions;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_geoarrow::PyCoordType;

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=65536, bbox=None, coord_type=None))]
pub fn read_flatgeobuf_async(
    py: Python,
    path: Bound<PyAny>,
    store: Option<Bound<PyAny>>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
    coord_type: Option<PyCoordType>,
) -> PyGeoArrowResult<PyObject> {
    let reader = construct_async_reader(path, store)?;
    let fut = future_into_py(py, async move {
        let options = FlatGeobufReaderOptions {
            batch_size: Some(batch_size),
            bbox,
            coord_type: coord_type.map(|x| x.into()).unwrap_or_default(),
        };
        let table = _read_flatgeobuf_async(reader.store, reader.path, options)
            .await
            .map_err(PyGeoArrowError::GeoArrowError)?;

        Ok(table_to_pytable(table))
    })?;
    Ok(fut.into())
}
