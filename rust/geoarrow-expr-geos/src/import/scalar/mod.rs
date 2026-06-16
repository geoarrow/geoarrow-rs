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
        match geom.get_coordinate_dimension().unwrap() {
            geos::CoordDimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::CoordDimensions::ThreeD => geo_traits::Dimensions::Xyz,
        }
}
