use pyo3::prelude::*;
pub mod algorithm;
pub mod array;
pub mod broadcasting;
pub mod chunked_array;
pub mod error;
pub mod ffi;
pub mod interop;
pub mod io;
pub mod scalar;
pub mod table;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    // Geometry scalars
    m.add_class::<scalar::Point>()?;
    m.add_class::<scalar::LineString>()?;
    m.add_class::<scalar::Polygon>()?;
    m.add_class::<scalar::MultiPoint>()?;
    m.add_class::<scalar::MultiLineString>()?;
    m.add_class::<scalar::MultiPolygon>()?;
    m.add_class::<scalar::Geometry>()?;
    m.add_class::<scalar::GeometryCollection>()?;
    m.add_class::<scalar::WKB>()?;
    m.add_class::<scalar::Rect>()?;

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
    m.add_class::<array::RectArray>()?;

    // Primitive arrays
    m.add_class::<array::BooleanArray>()?;
    // m.add_class::<array::Float16Array>()?;
    // m.add_class::<array::Float32Array>()?;
    m.add_class::<array::Float64Array>()?;
    // m.add_class::<array::Int16Array>()?;
    // m.add_class::<array::Int32Array>()?;
    // m.add_class::<array::Int64Array>()?;
    // m.add_class::<array::Int8Array>()?;
    // m.add_class::<array::LargeStringArray>()?;
    // m.add_class::<array::StringArray>()?;
    // m.add_class::<array::UInt16Array>()?;
    // m.add_class::<array::UInt32Array>()?;
    // m.add_class::<array::UInt64Array>()?;
    // m.add_class::<array::UInt8Array>()?;

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
    m.add_class::<chunked_array::ChunkedRectArray>()?;

    // Chunked primitive arrays
    m.add_class::<chunked_array::ChunkedBooleanArray>()?;
    // m.add_class::<chunked_array::ChunkedFloat16Array>()?;
    // m.add_class::<chunked_array::ChunkedFloat32Array>()?;
    m.add_class::<chunked_array::ChunkedFloat64Array>()?;
    // m.add_class::<chunked_array::ChunkedInt16Array>()?;
    // m.add_class::<chunked_array::ChunkedInt32Array>()?;
    // m.add_class::<chunked_array::ChunkedInt64Array>()?;
    // m.add_class::<chunked_array::ChunkedInt8Array>()?;
    // m.add_class::<chunked_array::ChunkedLargeStringArray>()?;
    // m.add_class::<chunked_array::ChunkedStringArray>()?;
    // m.add_class::<chunked_array::ChunkedUInt16Array>()?;
    // m.add_class::<chunked_array::ChunkedUInt32Array>()?;
    // m.add_class::<chunked_array::ChunkedUInt64Array>()?;
    // m.add_class::<chunked_array::ChunkedUInt8Array>()?;

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
        crate::algorithm::geo::chaikin_smoothing::chaikin_smoothing,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::chamberlain_duquette_area::chamberlain_duquette_signed_area,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::chamberlain_duquette_area::chamberlain_duquette_unsigned_area,
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
        crate::algorithm::geo::dimensions::is_empty,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::geodesic_area::geodesic_area_signed,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::geodesic_area::geodesic_area_unsigned,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::geodesic_area::geodesic_perimeter,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::simplify::simplify,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::algorithm::geo::simplify_vw::simplify_vw,
        m
    )?)?;

    // IO

    m.add_function(wrap_pyfunction!(crate::io::csv::read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::flatgeobuf::read_flatgeobuf, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::geojson::read_geojson, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::geojson_lines::read_geojson_lines,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::parquet::read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::postgis::read_postgis, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::postgis::read_postgis_async, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::pyogrio::from_pyogrio::read_pyogrio,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::csv::write_csv, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::io::flatgeobuf::write_flatgeobuf,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::geojson::write_geojson, m)?)?;

    // Interop
    m.add_function(wrap_pyfunction!(
        crate::interop::geopandas::from_geopandas::from_geopandas,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::geopandas::to_geopandas::to_geopandas,
        m
    )?)?;

    m.add_function(wrap_pyfunction!(crate::io::ewkb::from_ewkb, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::shapely::from_shapely::from_shapely,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::interop::shapely::to_shapely::to_shapely,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::io::wkb::from_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::wkb::to_wkb, m)?)?;
    m.add_function(wrap_pyfunction!(crate::io::wkt::from_wkt, m)?)?;

    // Exceptions
    // create_exception!(m, GeoArrowException, pyo3::exceptions::PyException);
    // m.add("GeoArrowException", py.get_type::<GeoArrowException>())?;

    Ok(())
}
