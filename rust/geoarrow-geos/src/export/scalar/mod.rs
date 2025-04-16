mod coord;
mod geometry;
mod geometrycollection;
mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;

pub(crate) use geometry::to_geos_geometry;

pub trait ToGEOSGeometry {
    fn to_geos_geometry(&self) -> std::result::Result<geos::Geometry, geos::Error>;
}
