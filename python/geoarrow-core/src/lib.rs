#![cfg_attr(not(test), deny(unused_crate_dependencies))]

mod constructors;
mod interop;
mod operations;

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

    m.add_class::<pyo3_geoarrow::PyGeoChunkedArray>()?;
    m.add_class::<pyo3_geoarrow::PyGeoArray>()?;
    m.add_class::<pyo3_geoarrow::PyGeoArrayReader>()?;
    m.add_class::<pyo3_geoarrow::PyGeoScalar>()?;
    m.add_class::<pyo3_geoarrow::data_type::PyGeoType>()?;

    // DataType constructors

    m.add_function(wrap_pyfunction!(crate::constructors::data_type::point, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::geometry,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::geometrycollection,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::linestring,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::multilinestring,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::multipoint,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::multipolygon,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::data_type::point, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::polygon,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::data_type::wkb, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::large_wkb,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::wkb_view,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::data_type::wkt, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::large_wkt,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::data_type::wkt_view,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::data_type::r#box, m)?)?;

    // Geometry Array Constructors

    m.add_function(wrap_pyfunction!(crate::constructors::array::points, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::array::linestrings,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::constructors::array::polygons, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::array::multipoints,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::array::multilinestrings,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::constructors::array::multipolygons,
        m
    )?)?;

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

    m.add_function(wrap_pyfunction!(
        crate::interop::from_shapely::from_shapely,
        m
    )?)?;
    // m.add_function(wrap_pyfunction!(
    //     crate::interop::shapely::to_shapely::to_shapely,
    //     m
    // )?)?;
    m.add_function(wrap_pyfunction!(crate::interop::from_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::interop::to_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::interop::from_wkt, m)?)?;
    m.add_function(wrap_pyfunction!(crate::interop::to_wkt, m)?)?;

    // Operations
    m.add_function(wrap_pyfunction!(
        crate::operations::type_id::get_type_id,
        m
    )?)?;

    // Exceptions
    // create_exception!(m, GeoArrowException, pyo3::exceptions::PyException);
    // m.add("GeoArrowException", py.get_type::<GeoArrowException>())?;

    Ok(())
}
