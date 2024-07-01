use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::interop::util::table_to_pytable;
use geoarrow::error::GeoArrowError;
use geoarrow::io::postgis::read_postgis as _read_postgis;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;
use sqlx::postgres::PgPoolOptions;

/// Read a PostGIS query into a GeoTable.
///
/// Returns:
///     Table from query.
#[pyfunction]
pub fn read_postgis(connection_url: String, sql: String) -> PyGeoArrowResult<Option<PyTable>> {
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

            Ok(table.map(table_to_pytable))
        })
}

/// Read a PostGIS query into a GeoTable.
///
/// Returns:
///     Table from query.
#[pyfunction]
pub fn read_postgis_async(
    py: Python,
    connection_url: String,
    sql: String,
) -> PyGeoArrowResult<PyObject> {
    let fut = pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
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
