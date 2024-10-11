use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};
use crate::io::geos::scalar::coord::GEOSConstCoord;
use crate::io::geos::scalar::{
    GEOSGeometryCollection, GEOSLineString, GEOSMultiLineString, GEOSMultiPoint, GEOSMultiPolygon,
    GEOSPoint, GEOSPolygon,
};
use crate::scalar::Geometry;
use geos::Geom;

impl<'a, const D: usize> TryFrom<&'a Geometry<'_, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a Geometry<'_, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        match value {
            Geometry::Point(g) => g.try_into(),
            Geometry::LineString(g) => g.try_into(),
            Geometry::Polygon(g) => g.try_into(),
            Geometry::MultiPoint(g) => g.try_into(),
            Geometry::MultiLineString(g) => g.try_into(),
            Geometry::MultiPolygon(g) => g.try_into(),
            Geometry::GeometryCollection(g) => g.try_into(),
            Geometry::Rect(_g) => todo!(),
        }
    }
}

#[derive(Clone)]
pub enum GEOSGeometry {
    Point(GEOSPoint),
    LineString(GEOSLineString),
    Polygon(GEOSPolygon),
    MultiPoint(GEOSMultiPoint),
    MultiLineString(GEOSMultiLineString),
    MultiPolygon(GEOSMultiPolygon),
    GeometryCollection(GEOSGeometryCollection),
}

impl GEOSGeometry {
    pub fn new(geom: geos::Geometry) -> Self {
        match geom.geometry_type() {
            geos::GeometryTypes::Point => Self::Point(GEOSPoint::new_unchecked(geom)),
            geos::GeometryTypes::LineString => {
                Self::LineString(GEOSLineString::new_unchecked(geom))
            }
            geos::GeometryTypes::Polygon => Self::Polygon(GEOSPolygon::new_unchecked(geom)),
            geos::GeometryTypes::MultiPoint => {
                Self::MultiPoint(GEOSMultiPoint::new_unchecked(geom))
            }
            geos::GeometryTypes::MultiLineString => {
                Self::MultiLineString(GEOSMultiLineString::new_unchecked(geom))
            }
            geos::GeometryTypes::MultiPolygon => {
                Self::MultiPolygon(GEOSMultiPolygon::new_unchecked(geom))
            }
            geos::GeometryTypes::GeometryCollection => {
                Self::GeometryCollection(GEOSGeometryCollection::new_unchecked(geom))
            }
            geos::GeometryTypes::LinearRing => panic!("GEOS Linear ring not supported"),
            geos::GeometryTypes::__Unknown(x) => panic!("Unknown geometry type {x}"),
        }
    }
}

pub struct GEOSRect {}

impl RectTrait for GEOSRect {
    type T = f64;
    type ItemType<'a> = GEOSConstCoord;

    fn dim(&self) -> crate::geo_traits::Dimension {
        todo!()
    }

    fn lower(&self) -> Self::ItemType<'_> {
        todo!()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        todo!()
    }
}

impl GeometryTrait for GEOSGeometry {
    type T = f64;
    type Point<'a> = GEOSPoint;
    type LineString<'a> = GEOSLineString;
    type Polygon<'a> = GEOSPolygon;
    type MultiPoint<'a> = GEOSMultiPoint;
    type MultiLineString<'a> = GEOSMultiLineString;
    type MultiPolygon<'a> = GEOSMultiPolygon;
    type GeometryCollection<'a> = GEOSGeometryCollection;
    type Rect<'a> = GEOSRect;

    fn dim(&self) -> crate::geo_traits::Dimension {
        match self {
            Self::Point(g) => g.dim(),
            Self::LineString(g) => g.dim(),
            Self::Polygon(g) => g.dim(),
            Self::MultiPoint(g) => g.dim(),
            Self::MultiLineString(g) => g.dim(),
            Self::MultiPolygon(g) => g.dim(),
            Self::GeometryCollection(g) => g.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        GEOSPoint,
        GEOSLineString,
        GEOSPolygon,
        GEOSMultiPoint,
        GEOSMultiLineString,
        GEOSMultiPolygon,
        GEOSGeometryCollection,
        Self::Rect<'_>,
    > {
        match self {
            Self::Point(g) => crate::geo_traits::GeometryType::Point(g),
            Self::LineString(g) => crate::geo_traits::GeometryType::LineString(g),
            Self::Polygon(g) => crate::geo_traits::GeometryType::Polygon(g),
            Self::MultiPoint(g) => crate::geo_traits::GeometryType::MultiPoint(g),
            Self::MultiLineString(g) => crate::geo_traits::GeometryType::MultiLineString(g),
            Self::MultiPolygon(g) => crate::geo_traits::GeometryType::MultiPolygon(g),
            Self::GeometryCollection(g) => crate::geo_traits::GeometryType::GeometryCollection(g),
        }
    }
}
