use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::table::GeoTable;
use geoarrow::error::GeoArrowError;
use geoarrow::io::postgis::read_postgis as _read_postgis;
use pyo3::prelude::*;
use sqlx::postgres::PgPoolOptions;

/// Read a PostGIS query into a GeoTable.
///
/// Returns:
///     Table from query.
#[pyfunction]
pub fn read_postgis(connection_url: String, sql: String) -> PyGeoArrowResult<Option<GeoTable>> {
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

            Ok(table.map(GeoTable))
        })

    // block_in_place(move || {
    //     Handle::current().block_on(async move {
    //         let pool = PgPoolOptions::new()
    //             .connect(&connection_url)
    //             .await
    //             .map_err(|err| PyGeoArrowError::GeoArrowError(GeoArrowError::SqlxError(err)))?;

    //         let table = _read_postgis(&pool, &sql)
    //             .await
    //             .map_err(PyGeoArrowError::GeoArrowError)?;

    //         Ok(table.map(GeoTable))
    //     })
    // })
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
    let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
        let pool = PgPoolOptions::new()
            .connect(&connection_url)
            .await
            .map_err(|err| PyGeoArrowError::GeoArrowError(GeoArrowError::SqlxError(err)))?;

        let table = _read_postgis(&pool, &sql)
            .await
            .map_err(PyGeoArrowError::GeoArrowError)?;

        Ok(table.map(GeoTable))
    })?;
    Ok(fut.into())
}
