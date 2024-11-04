use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::util::table_to_pytable;
use geoarrow::error::GeoArrowError;
use geoarrow::io::postgis::read_postgis as _read_postgis;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use sqlx::postgres::PgPoolOptions;

#[pyfunction]
pub fn read_postgis(
    py: Python,
    connection_url: String,
    sql: String,
) -> PyGeoArrowResult<Option<PyObject>> {
    // https://tokio.rs/tokio/topics/bridging#what-tokiomain-expands-to
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let pool = PgPoolOptions::new()
                .connect(&connection_url)
                .await
                .map_err(|err| PyGeoArrowError::GeoArrowError(GeoArrowError::SqlxError(err)))?;

            let table = _read_postgis(&pool, &sql)
                .await
                .map_err(PyGeoArrowError::GeoArrowError)?;

            Ok(table
                .map(|table| table_to_pytable(table).to_arro3(py))
                .transpose()?)
        })
}

#[pyfunction]
pub fn read_postgis_async(
    py: Python,
    connection_url: String,
    sql: String,
) -> PyGeoArrowResult<PyObject> {
    let fut = future_into_py(py, async move {
        let pool = PgPoolOptions::new()
            .connect(&connection_url)
            .await
            .map_err(|err| PyGeoArrowError::GeoArrowError(GeoArrowError::SqlxError(err)))?;

        let table = _read_postgis(&pool, &sql)
            .await
            .map_err(PyGeoArrowError::GeoArrowError)?;

        Ok(table.map(table_to_pytable))
    })?;
    Ok(fut.into())
}
