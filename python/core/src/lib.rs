use pyo3::prelude::*;
pub mod algorithm;
pub mod array;
pub mod broadcasting;
pub mod chunked_array;
pub mod ffi;
pub mod io;
pub mod table;

/// A Python module implemented in Rust.
#[pymodule]
fn rust(_py: Python, m: &PyModule) -> PyResult<()> {
    // Geometry arrays
    m.add_class::<array::PointArray>()?;
    m.add_class::<array::LineStringArray>()?;
    m.add_class::<array::PolygonArray>()?;
    m.add_class::<array::MultiPointArray>()?;
    m.add_class::<array::MultiLineStringArray>()?;
    m.add_class::<array::MultiPolygonArray>()?;
    m.add_class::<array::MixedGeometryArray>()?;
    m.add_class::<array::GeometryCollectionArray>()?;
    m.add_class::<array::WKBArray>()?;

    // Primitive arrays
    m.add_class::<array::BooleanArray>()?;
    m.add_class::<array::Float16Array>()?;
    m.add_class::<array::Float32Array>()?;
    m.add_class::<array::Float64Array>()?;
    m.add_class::<array::Int16Array>()?;
    m.add_class::<array::Int32Array>()?;
    m.add_class::<array::Int64Array>()?;
    m.add_class::<array::Int8Array>()?;
    m.add_class::<array::LargeStringArray>()?;
    m.add_class::<array::StringArray>()?;
    m.add_class::<array::UInt16Array>()?;
    m.add_class::<array::UInt32Array>()?;
    m.add_class::<array::UInt64Array>()?;
    m.add_class::<array::UInt8Array>()?;

    // Chunked geometry arrays
    m.add_class::<chunked_array::ChunkedPointArray>()?;
    m.add_class::<chunked_array::ChunkedLineStringArray>()?;
    m.add_class::<chunked_array::ChunkedPolygonArray>()?;
    m.add_class::<chunked_array::ChunkedMultiPointArray>()?;
    m.add_class::<chunked_array::ChunkedMultiLineStringArray>()?;
    m.add_class::<chunked_array::ChunkedMultiPolygonArray>()?;
    m.add_class::<chunked_array::ChunkedMixedGeometryArray>()?;
    m.add_class::<chunked_array::ChunkedGeometryCollectionArray>()?;
    m.add_class::<chunked_array::ChunkedWKBArray>()?;

    // Chunked primitive arrays
    m.add_class::<chunked_array::ChunkedBooleanArray>()?;
    m.add_class::<chunked_array::ChunkedFloat16Array>()?;
    m.add_class::<chunked_array::ChunkedFloat32Array>()?;
    m.add_class::<chunked_array::ChunkedFloat64Array>()?;
    m.add_class::<chunked_array::ChunkedInt16Array>()?;
    m.add_class::<chunked_array::ChunkedInt32Array>()?;
    m.add_class::<chunked_array::ChunkedInt64Array>()?;
    m.add_class::<chunked_array::ChunkedInt8Array>()?;
    m.add_class::<chunked_array::ChunkedLargeStringArray>()?;
    m.add_class::<chunked_array::ChunkedStringArray>()?;
    m.add_class::<chunked_array::ChunkedUInt16Array>()?;
    m.add_class::<chunked_array::ChunkedUInt32Array>()?;
    m.add_class::<chunked_array::ChunkedUInt64Array>()?;
    m.add_class::<chunked_array::ChunkedUInt8Array>()?;

    // Table
    m.add_class::<table::GeoTable>()?;

    // Top-level functions
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
        crate::algorithm::geo::convex_hull::convex_hull,
        m
    )?)?;

    // IO
    m.add_function(wrap_pyfunction!(crate::io::wkb::to_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::wkb::from_wkb, m)?)?;

    m.add_function(wrap_pyfunction!(crate::io::csv::read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::flatgeobuf::read_flatgeobuf, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::geojson::read_geojson, m)?)?;

    Ok(())
}
