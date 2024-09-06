use pyo3::prelude::*;
mod algorithm;
pub mod broadcasting;
pub mod ffi;
mod util;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _compute(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
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
