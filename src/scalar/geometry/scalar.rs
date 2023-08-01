use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

#[derive(Debug, Clone, PartialEq)]
pub enum Geometry<'a, O: Offset> {
    Point(Point<'a>),
    LineString(LineString<'a, O>),
    Polygon(Polygon<'a, O>),
    MultiPoint(MultiPoint<'a, O>),
    MultiLineString(MultiLineString<'a, O>),
    MultiPolygon(MultiPolygon<'a, O>),
    GeometryCollection(GeometryCollection<'a, O>),
    WKB(WKB<'a, O>),
    Rect(Rect<'a>),
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for Geometry<'a, O> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        use Geometry::*;
        match self {
            Point(g) => geo::Geometry::Point(g.into()),
            LineString(g) => geo::Geometry::LineString(g.into()),
            Polygon(g) => geo::Geometry::Polygon(g.into()),
            MultiPoint(g) => geo::Geometry::MultiPoint(g.into()),
            MultiLineString(g) => geo::Geometry::MultiLineString(g.into()),
            MultiPolygon(g) => geo::Geometry::MultiPolygon(g.into()),
            GeometryCollection(g) => geo::Geometry::GeometryCollection(g.into()),
            WKB(g) => g.into(),
            Rect(g) => geo::Geometry::Rect(g.into()),
        }
    }
}

impl<'a, O: Offset> GeometryTrait<'a> for Geometry<'a, O> {
    type T = f64;
    type Point = Point<'a>;
    type LineString = LineString<'a, O>;
    type Polygon = Polygon<'a, O>;
    type MultiPoint = MultiPoint<'a, O>;
    type MultiLineString = MultiLineString<'a, O>;
    type MultiPolygon = MultiPolygon<'a, O>;
    type GeometryCollection = GeometryCollection<'a, O>;
    type Rect = Rect<'a>;

    // TODO: not 100% sure what this is
    #[allow(implied_bounds_entailment)]
    fn as_type(
        &'a self,
    ) -> crate::geo_traits::GeometryType<
        'a,
        Point,
        LineString<O>,
        Polygon<O>,
        MultiPoint<O>,
        MultiLineString<O>,
        MultiPolygon<O>,
        GeometryCollection<O>,
        Rect,
    > {
        use Geometry::*;
        match self {
            Point(p) => GeometryType::Point(p),
            LineString(p) => GeometryType::LineString(p),
            Polygon(p) => GeometryType::Polygon(p),
            MultiPoint(p) => GeometryType::MultiPoint(p),
            MultiLineString(p) => GeometryType::MultiLineString(p),
            MultiPolygon(p) => GeometryType::MultiPolygon(p),
            GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Rect(p) => GeometryType::Rect(p),
            _ => todo!(),
        }
    }
}

impl<O: Offset> RTreeObject for Geometry<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        use Geometry::*;
        match self {
            Point(geom) => geom.envelope(),
            LineString(geom) => geom.envelope(),
            Polygon(geom) => geom.envelope(),
            MultiPoint(geom) => geom.envelope(),
            MultiLineString(geom) => geom.envelope(),
            MultiPolygon(geom) => geom.envelope(),
            GeometryCollection(geom) => geom.envelope(),
            WKB(geom) => geom.envelope(),
            Rect(geom) => geom.envelope(),
        }
    }
}

impl<O: Offset> From<Geometry<'_, O>> for geo::Geometry {
    fn from(value: Geometry<'_, O>) -> Self {
        use Geometry::*;
        match value {
            Point(geom) => geom.into(),
            LineString(geom) => geom.into(),
            Polygon(geom) => geom.into(),
            MultiPoint(geom) => geom.into(),
            MultiLineString(geom) => geom.into(),
            MultiPolygon(geom) => geom.into(),
            GeometryCollection(geom) => geo::Geometry::GeometryCollection(geom.into()),
            WKB(geom) => geom.into(),
            Rect(geom) => geom.into(),
        }
    }
}
