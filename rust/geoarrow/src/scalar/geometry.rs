use crate::algorithm::native::eq::geometry_eq;
use crate::scalar::*;
use crate::trait_::NativeScalar;
use geo_traits::to_geo::ToGeoGeometry;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait, UnimplementedLine,
    UnimplementedTriangle,
};
use rstar::{RTreeObject, AABB};

/// A Geometry is an enum over the various underlying _zero copy_ GeoArrow scalar types.
#[derive(Debug)]
pub enum Geometry {
    Point(crate::scalar::Point),
    LineString(crate::scalar::LineString),
    Polygon(crate::scalar::Polygon),
    MultiPoint(crate::scalar::MultiPoint),
    MultiLineString(crate::scalar::MultiLineString),
    MultiPolygon(crate::scalar::MultiPolygon),
    GeometryCollection(crate::scalar::GeometryCollection),
    Rect(crate::scalar::Rect),
}

impl NativeScalar for Geometry {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        match self {
            Geometry::Point(g) => geo::Geometry::Point(g.into()),
            Geometry::LineString(g) => geo::Geometry::LineString(g.into()),
            Geometry::Polygon(g) => geo::Geometry::Polygon(g.into()),
            Geometry::MultiPoint(g) => geo::Geometry::MultiPoint(g.into()),
            Geometry::MultiLineString(g) => geo::Geometry::MultiLineString(g.into()),
            Geometry::MultiPolygon(g) => geo::Geometry::MultiPolygon(g.into()),
            Geometry::GeometryCollection(g) => geo::Geometry::GeometryCollection(g.into()),
            Geometry::Rect(g) => geo::Geometry::Rect(g.into()),
        }
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        self.to_geo()
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl GeometryTrait for Geometry {
    type T = f64;
    type PointType<'b>
        = Point
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection
    where
        Self: 'b;
    type RectType<'b>
        = Rect
    where
        Self: 'b;
    type LineType<'b>
        = UnimplementedLine<f64>
    where
        Self: 'b;
    type TriangleType<'b>
        = UnimplementedTriangle<f64>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Geometry::Point(p) => p.dim(),
            Geometry::LineString(p) => p.dim(),
            Geometry::Polygon(p) => p.dim(),
            Geometry::MultiPoint(p) => p.dim(),
            Geometry::MultiLineString(p) => p.dim(),
            Geometry::MultiPolygon(p) => p.dim(),
            Geometry::GeometryCollection(p) => p.dim(),
            Geometry::Rect(p) => p.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        Point,
        LineString,
        Polygon,
        MultiPoint,
        MultiLineString,
        MultiPolygon,
        GeometryCollection,
        Rect,
        UnimplementedTriangle<f64>,
        UnimplementedLine<f64>,
    > {
        match self {
            Geometry::Point(p) => GeometryType::Point(p),
            Geometry::LineString(p) => GeometryType::LineString(p),
            Geometry::Polygon(p) => GeometryType::Polygon(p),
            Geometry::MultiPoint(p) => GeometryType::MultiPoint(p),
            Geometry::MultiLineString(p) => GeometryType::MultiLineString(p),
            Geometry::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            Geometry::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Geometry::Rect(p) => GeometryType::Rect(p),
        }
    }
}

impl<'a> GeometryTrait for &'a Geometry {
    type T = f64;
    type PointType<'b>
        = Point
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection
    where
        Self: 'b;
    type RectType<'b>
        = Rect
    where
        Self: 'b;
    type LineType<'b>
        = UnimplementedLine<f64>
    where
        Self: 'b;
    type TriangleType<'b>
        = UnimplementedTriangle<f64>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Geometry::Point(p) => p.dim(),
            Geometry::LineString(p) => p.dim(),
            Geometry::Polygon(p) => p.dim(),
            Geometry::MultiPoint(p) => p.dim(),
            Geometry::MultiLineString(p) => p.dim(),
            Geometry::MultiPolygon(p) => p.dim(),
            Geometry::GeometryCollection(p) => p.dim(),
            Geometry::Rect(p) => p.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        'a,
        Point,
        LineString,
        Polygon,
        MultiPoint,
        MultiLineString,
        MultiPolygon,
        GeometryCollection,
        Rect,
        UnimplementedTriangle<f64>,
        UnimplementedLine<f64>,
    > {
        match self {
            Geometry::Point(p) => GeometryType::Point(p),
            Geometry::LineString(p) => GeometryType::LineString(p),
            Geometry::Polygon(p) => GeometryType::Polygon(p),
            Geometry::MultiPoint(p) => GeometryType::MultiPoint(p),
            Geometry::MultiLineString(p) => GeometryType::MultiLineString(p),
            Geometry::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            Geometry::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Geometry::Rect(p) => GeometryType::Rect(p),
        }
    }
}

impl RTreeObject for Geometry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match self {
            Geometry::Point(geom) => geom.envelope(),
            Geometry::LineString(geom) => geom.envelope(),
            Geometry::Polygon(geom) => geom.envelope(),
            Geometry::MultiPoint(geom) => geom.envelope(),
            Geometry::MultiLineString(geom) => geom.envelope(),
            Geometry::MultiPolygon(geom) => geom.envelope(),
            Geometry::GeometryCollection(geom) => geom.envelope(),
            Geometry::Rect(geom) => geom.envelope(),
        }
    }
}

impl From<Geometry> for geo::Geometry {
    fn from(value: Geometry) -> Self {
        (&value).into()
    }
}

impl From<&Geometry> for geo::Geometry {
    fn from(value: &Geometry) -> Self {
        ToGeoGeometry::to_geometry(value)
    }
}

impl<G: GeometryTrait<T = f64>> PartialEq<G> for Geometry {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
