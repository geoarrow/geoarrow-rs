#![cfg_attr(not(test), deny(unused_crate_dependencies))]

mod constructors;
// pub mod ffi;
// pub mod interop;
// pub mod table;

use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

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
            "geoarrow-rust-core has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<pyo3_geoarrow::PyGeoArrowArray>()?;
    m.add_class::<pyo3_geoarrow::PyChunkedGeoArrowArray>()?;
    m.add_class::<pyo3_geoarrow::PyGeoArrowType>()?;

    // Type constructors

    m.add_function(wrap_pyfunction!(pyo3_geoarrow::point, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::geometry, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::geometrycollection, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::linestring, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::multilinestring, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::multipoint, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::multipolygon, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::point, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::polygon, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::wkb, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::wkt, m)?)?;
    m.add_function(wrap_pyfunction!(pyo3_geoarrow::r#box, m)?)?;

    // Constructors

    m.add_function(wrap_pyfunction!(crate::constructors::points, m)?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::linestrings, m)?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::polygons, m)?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::multipoints, m)?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::multilinestrings, m)?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::multipolygons, m)?)?;

    // Top-level table functions

    // m.add_function(wrap_pyfunction!(crate::table::geometry_col, m)?)?;

    // Interop

    // m.add_function(wrap_pyfunction!(
    //     crate::interop::pyogrio::from_pyogrio::read_pyogrio,
    //     m
    // )?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::interop::geopandas::from_geopandas::from_geopandas,
    //     m
    // )?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::interop::geopandas::to_geopandas::to_geopandas,
    //     m
    // )?)?;

    // m.add_function(wrap_pyfunction!(
    //     crate::interop::shapely::from_shapely::from_shapely,
    //     m
    // )?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::interop::shapely::to_shapely::to_shapely,
    //     m
    // )?)?;
    // m.add_function(wrap_pyfunction!(crate::interop::wkb::from_wkb, m)?)?;
    // m.add_function(wrap_pyfunction!(crate::interop::wkb::to_wkb, m)?)?;
    // m.add_function(wrap_pyfunction!(crate::interop::wkt::from_wkt, m)?)?;
    // m.add_function(wrap_pyfunction!(crate::interop::wkt::to_wkt, m)?)?;

    // Exceptions
    // create_exception!(m, GeoArrowException, pyo3::exceptions::PyException);
    // m.add("GeoArrowException", py.get_type::<GeoArrowException>())?;

    Ok(())
}
