#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
mod algorithm;
pub mod ffi;
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
            "geoarrow-rust-compute has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _compute(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;
    m.add_wrapped(wrap_pyfunction!(___version))?;

    // Top-level array/chunked array functions
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::affine_ops::affine_transform,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::area::area, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::area::signed_area,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::center::center, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::centroid::centroid,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::chaikin_smoothing::chaikin_smoothing,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::convex_hull::convex_hull,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::densify::densify,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::envelope::envelope,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::frechet_distance::frechet_distance,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::dimensions::is_empty,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::geodesic_area::geodesic_perimeter,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::length::length, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::line_interpolate_point::line_interpolate_point,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::line_locate_point::line_locate_point,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::rotate::rotate, m)?)?;
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::scale::scale, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::simplify::simplify,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::algorithm::geo::skew::skew, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::translate::translate,
        m
    )?)?;

    // Native functions
    m.add_function(wrap_pyfunction!(
        crate::algorithm::native::total_bounds::total_bounds,
        m
    )?)?;

    #[cfg(feature = "libc")]
    m.add_function(wrap_pyfunction!(crate::algorithm::polylabel::polylabel, m)?)?;

    // Top-level table functions

    m.add_function(wrap_pyfunction!(
        crate::algorithm::native::explode::explode,
        m
    )?)?;

    Ok(())
}
