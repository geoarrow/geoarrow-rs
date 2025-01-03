use crate::algorithm::native::eq::geometry_eq;
use crate::io::geo::geometry_to_geo;
use crate::scalar::*;
use crate::trait_::NativeScalar;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait, UnimplementedLine,
    UnimplementedTriangle,
};
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Geometry
///
/// This implements [GeometryTrait], which you can use to extract data.
#[derive(Debug)]
pub enum Geometry<'a> {
    /// Point geometry
    Point(crate::scalar::Point<'a>),
    /// LineString geometry
    LineString(crate::scalar::LineString<'a>),
    /// Polygon geometry
    Polygon(crate::scalar::Polygon<'a>),
    /// MultiPoint geometry
    MultiPoint(crate::scalar::MultiPoint<'a>),
    /// MultiLineString geometry
    MultiLineString(crate::scalar::MultiLineString<'a>),
    /// MultiPolygon geometry
    MultiPolygon(crate::scalar::MultiPolygon<'a>),
    /// GeometryCollection geometry
    GeometryCollection(crate::scalar::GeometryCollection<'a>),
    /// Rect geometry
    Rect(crate::scalar::Rect<'a>),
}

impl NativeScalar for Geometry<'_> {
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

impl GeometryTrait for Geometry<'_> {
    type T = f64;
    type PointType<'b>
        = Point<'b>
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString<'b>
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon<'b>
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint<'b>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString<'b>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon<'b>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection<'b>
    where
        Self: 'b;
    type RectType<'b>
        = Rect<'b>
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

    #[inline]
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

    #[inline]
    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        Point<'_>,
        LineString<'_>,
        Polygon<'_>,
        MultiPoint<'_>,
        MultiLineString<'_>,
        MultiPolygon<'_>,
        GeometryCollection<'_>,
        Rect<'_>,
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

impl<'a> GeometryTrait for &'a Geometry<'a> {
    type T = f64;
    type PointType<'b>
        = Point<'b>
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString<'b>
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon<'b>
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint<'b>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString<'b>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon<'b>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection<'b>
    where
        Self: 'b;
    type RectType<'b>
        = Rect<'b>
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

    #[inline]
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

    #[inline]
    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        'a,
        Point<'a>,
        LineString<'a>,
        Polygon<'a>,
        MultiPoint<'a>,
        MultiLineString<'a>,
        MultiPolygon<'a>,
        GeometryCollection<'a>,
        Rect<'a>,
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
            Geometry::GeometryCollection(geom) => geom.envelope(),
            Geometry::Rect(geom) => geom.envelope(),
        }
    }
}

impl From<Geometry<'_>> for geo::Geometry {
    fn from(value: Geometry<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Geometry<'_>> for geo::Geometry {
    fn from(value: &Geometry<'_>) -> Self {
        geometry_to_geo(value)
    }
}

impl<G: GeometryTrait<T = f64>> PartialEq<G> for Geometry<'_> {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
