use rstar::{RTreeObject, AABB};

use crate::Geometry;

impl RTreeObject for Geometry<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match self {
            Geometry::Point(geom) => geom.envelope(),
            Geometry::LineString(geom) => geom.envelope(),
            Geometry::Polygon(geom) => geom.envelope(),
            Geometry::MultiPoint(geom) => geom.envelope(),
            Geometry::MultiLineString(geom) => geom.envelope(),
            Geometry::MultiPolygon(geom) => geom.envelope(),
            Geometry::WKB(geom) => geom.envelope(),
        }
    }
}

impl From<Geometry<'_>> for geo::Geometry {
    fn from(value: Geometry) -> Self {
        match value {
            Geometry::Point(geom) => geom.into(),
            Geometry::LineString(geom) => geom.into(),
            Geometry::Polygon(geom) => geom.into(),
            Geometry::MultiPoint(geom) => geom.into(),
            Geometry::MultiLineString(geom) => geom.into(),
            Geometry::MultiPolygon(geom) => geom.into(),
            Geometry::WKB(geom) => geom.into(),
        }
    }
}
