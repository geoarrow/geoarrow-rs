use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
pub mod error;
// pub mod ffi;
pub mod io;
#[cfg(feature = "async")]
mod runtime;
mod util;

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
        pyo3_object_store::register_store_module(py, m, "geoarrow.rust.io")?;
        pyo3_object_store::register_exceptions_module(py, m, "geoarrow.rust.io")?;

        m.add_class::<crate::io::parquet::ParquetFile>()?;
        m.add_class::<crate::io::parquet::ParquetDataset>()?;

        m.add_function(wrap_pyfunction!(
            crate::io::flatgeobuf::read_flatgeobuf_async,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(crate::io::parquet::read_parquet_async, m)?)?;

        m.add_function(wrap_pyfunction!(crate::io::postgis::read_postgis, m)?)?;
        m.add_function(wrap_pyfunction!(crate::io::postgis::read_postgis_async, m)?)?;
    }

    // IO

    m.add_function(wrap_pyfunction!(crate::io::csv::read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::flatgeobuf::read_flatgeobuf, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::geojson::read_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::geojson_lines::read_geojson_lines,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::parquet::read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::parquet::write_parquet, m)?)?;
    m.add_class::<crate::io::parquet::ParquetWriter>()?;

    m.add_function(wrap_pyfunction!(crate::io::csv::write_csv, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::flatgeobuf::write_flatgeobuf,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::geojson::write_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::geojson_lines::write_geojson_lines,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::shapefile::read_shapefile, m)?)?;

    Ok(())
}
