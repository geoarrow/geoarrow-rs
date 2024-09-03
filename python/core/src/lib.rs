use pyo3::prelude::*;
pub mod algorithm;
pub mod array;
pub mod broadcasting;
pub mod chunked_array;
pub(crate) mod crs;
pub mod error;
pub mod ffi;
pub mod interop;
pub mod scalar;
pub mod table;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<scalar::PyGeometry>()?;
    m.add_class::<array::PyGeometryArray>()?;
    m.add_class::<chunked_array::PyChunkedGeometryArray>()?;

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
    m.add_function(wrap_pyfunction!(crate::algorithm::polylabel::polylabel, m)?)?;

    // Top-level table functions

    m.add_function(wrap_pyfunction!(
        crate::algorithm::native::explode::explode,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::table::geometry_col, m)?)?;

    // Interop

    m.add_function(wrap_pyfunction!(
        crate::interop::pyogrio::from_pyogrio::read_pyogrio,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::geopandas::from_geopandas::from_geopandas,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::geopandas::to_geopandas::to_geopandas,
        m
    )?)?;

    m.add_function(wrap_pyfunction!(crate::interop::ewkb::from_ewkb, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::shapely::from_shapely::from_shapely,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::shapely::to_shapely::to_shapely,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::interop::wkb::from_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::interop::wkb::to_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::interop::wkt::from_wkt, m)?)?;

    // Exceptions
    // create_exception!(m, GeoArrowException, pyo3::exceptions::PyException);
    // m.add("GeoArrowException", py.get_type::<GeoArrowException>())?;

    Ok(())
}
