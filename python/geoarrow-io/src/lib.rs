#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
pub mod error;
pub(crate) mod input;
pub mod parquet;
#[cfg(feature = "async")]
mod runtime;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// Raise RuntimeWarning for debug builds
#[pyfunction]
fn check_debug_build(py: Python) -> PyResult<()> {
    #[cfg(debug_assertions)]
    {
        let warnings_mod = py.import(intern!(py, "warnings"))?;
        let warning = PyRuntimeWarning::new_err(
            "geoarrow-rust-io has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _io(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;
    m.add_wrapped(wrap_pyfunction!(___version))?;

    // Async IO

    #[cfg(feature = "async")]
    {
        pyo3_object_store::register_store_module(py, m, "geoarrow.rust.io", "store")?;
        pyo3_object_store::register_exceptions_module(py, m, "geoarrow.rust.io", "exceptions")?;

        m.add_class::<crate::parquet::GeoParquetFile>()?;
        m.add_class::<crate::parquet::GeoParquetDataset>()?;

        // m.add_function(wrap_pyfunction!(
        //     crate::flatgeobuf::read_flatgeobuf_async,
        //     m
        // )?)?;
        m.add_function(wrap_pyfunction!(crate::parquet::read_parquet_async, m)?)?;

        // m.add_function(wrap_pyfunction!(crate::postgis::read_postgis, m)?)?;
        // m.add_function(wrap_pyfunction!(crate::postgis::read_postgis_async, m)?)?;
    }

    // IO

    // m.add_function(wrap_pyfunction!(crate::csv::read_csv, m)?)?;
    // m.add_function(wrap_pyfunction!(crate::flatgeobuf::read_flatgeobuf, m)?)?;
    // m.add_function(wrap_pyfunction!(crate::geojson::read_geojson, m)?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::geojson_lines::read_geojson_lines,
    //     m
    // )?)?;
    m.add_function(wrap_pyfunction!(crate::parquet::read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(crate::parquet::write_parquet, m)?)?;
    m.add_class::<crate::parquet::PyGeoParquetWriter>()?;

    // m.add_function(wrap_pyfunction!(crate::csv::write_csv, m)?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::flatgeobuf::write_flatgeobuf,
    //     m
    // )?)?;
    // m.add_function(wrap_pyfunction!(crate::geojson::write_geojson, m)?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::geojson_lines::write_geojson_lines,
    //     m
    // )?)?;
    // m.add_function(wrap_pyfunction!(crate::shapefile::read_shapefile, m)?)?;

    Ok(())
}
