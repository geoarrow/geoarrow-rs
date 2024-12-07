use crate::error::PyGeoArrowError;
use crate::util::to_arro3_table;

use geoarrow::error::GeoArrowError;
use geoarrow::io::postgis::read_postgis as _read_postgis;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;
use pyo3_async_runtimes::tokio::future_into_py;
use sqlx::postgres::PgPoolOptions;

#[pyfunction]
pub fn read_postgis(connection_url: String, sql: String) -> PyResult<Option<Arro3Table>> {
    // https://tokio.rs/tokio/topics/bridging#what-tokiomain-expands-to
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // TODO: py.allow_threads
    runtime.block_on(read_postgis_inner(connection_url, sql))
}

#[pyfunction]
pub fn read_postgis_async(
    py: Python,
    connection_url: String,
    sql: String,
) -> PyResult<Bound<PyAny>> {
    future_into_py(py, read_postgis_inner(connection_url, sql))
}

async fn read_postgis_inner(connection_url: String, sql: String) -> PyResult<Option<Arro3Table>> {
    let pool = PgPoolOptions::new()
        .connect(&connection_url)
        .await
        .map_err(|err| PyGeoArrowError::GeoArrowError(GeoArrowError::SqlxError(err)))?;

    let table = _read_postgis(&pool, &sql)
        .await
        .map_err(PyGeoArrowError::GeoArrowError)?;

    Ok(table.map(to_arro3_table))
}
