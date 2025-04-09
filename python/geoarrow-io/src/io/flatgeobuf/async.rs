use crate::error::PyGeoArrowError;
use crate::io::input::construct_async_reader;
use crate::util::to_arro3_table;

use geoarrow::io::flatgeobuf::FlatGeobufReaderOptions;
use geoarrow::io::flatgeobuf::read_flatgeobuf_async as _read_flatgeobuf_async;
use geoarrow_schema::CoordType;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_geoarrow::PyCoordType;

#[pyfunction]
#[pyo3(signature = (path, *, store=None, batch_size=65536, bbox=None, coord_type=None))]
pub fn read_flatgeobuf_async<'py>(
    py: Python<'py>,
    path: Bound<'py, PyAny>,
    store: Option<Bound<'py, PyAny>>,
    batch_size: usize,
    bbox: Option<(f64, f64, f64, f64)>,
    coord_type: Option<PyCoordType>,
) -> PyResult<Bound<'py, PyAny>> {
    let reader = construct_async_reader(path, store)?;
    future_into_py(py, async move {
        let options = FlatGeobufReaderOptions {
            batch_size: Some(batch_size),
            bbox,
            coord_type: coord_type
                .map(|x| x.into())
                .unwrap_or(CoordType::default_interleaved()),
        };
        let table = _read_flatgeobuf_async(reader.store, reader.path, options)
            .await
            .map_err(PyGeoArrowError::GeoArrowError)?;
        Ok(to_arro3_table(table))
    })
}
