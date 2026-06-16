mod coord;
mod geometry;
mod geometrycollection;
mod linearring;
mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;

pub use geometry::GEOSGeometry;
pub use geometrycollection::GEOSGeometryCollection;
pub use linearring::GEOSConstLinearRing;
pub use linestring::{GEOSConstLineString, GEOSLineString};
pub use multilinestring::GEOSMultiLineString;
pub use multipoint::GEOSMultiPoint;
pub use multipolygon::GEOSMultiPolygon;
pub use point::{GEOSConstPoint, GEOSPoint};
pub use polygon::{GEOSConstPolygon, GEOSPolygon};

/// Determine the [`geo_traits::Dimensions`] of a GEOS geometry.
pub(crate) fn dimensions_from_geom(geom: &impl geos::Geom) -> geo_traits::Dimensions {
    #[cfg(not(feature = "geos-3_14"))]
    {
        match geom.get_coordinate_dimension().unwrap() {
            geos::CoordDimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::CoordDimensions::ThreeD => geo_traits::Dimensions::Xyz,
        }
    }
    #[cfg(feature = "geos-3_14")]
    {
        match (geom.has_z().unwrap(), geom.has_m().unwrap()) {
            (false, false) => geo_traits::Dimensions::Xy,
            (true, false) => geo_traits::Dimensions::Xyz,
            (false, true) => geo_traits::Dimensions::Xym,
            (true, true) => geo_traits::Dimensions::Xyzm,
        }
    }
}
