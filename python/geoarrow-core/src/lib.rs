use pyo3::prelude::*;
pub(crate) mod crs;
pub mod ffi;
pub mod interop;
// pub mod scalar;
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

    m.add_class::<pyo3_geoarrow::PyGeometry>()?;
    m.add_class::<pyo3_geoarrow::PyGeometryArray>()?;
    m.add_class::<pyo3_geoarrow::PyChunkedGeometryArray>()?;
    m.add_class::<pyo3_geoarrow::PyGeometryType>()?;

    // Top-level table functions

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
