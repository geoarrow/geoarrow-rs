use pyo3::prelude::*;
pub(crate) mod crs;
pub mod error;
// pub mod ffi;
pub mod io;
mod util;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _io(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<crate::io::object_store::PyObjectStore>()?;
    m.add_class::<crate::io::parquet::reader::ParquetFile>()?;
    m.add_class::<crate::io::parquet::reader::ParquetDataset>()?;

    m.add_function(wrap_pyfunction!(crate::io::csv::read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::flatgeobuf::read_flatgeobuf, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::flatgeobuf::read_flatgeobuf_async,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::geojson::read_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::geojson_lines::read_geojson_lines,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::parquet::reader::read_parquet,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::parquet::reader::read_parquet_async,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::parquet::writer::write_parquet,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::postgis::read_postgis, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::postgis::read_postgis_async, m)?)?;
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

    Ok(())
}
