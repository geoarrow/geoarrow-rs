use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

/// A Geometry is an enum over the various underlying _zero copy_ GeoArrow scalar types.
///
/// Notably this does _not_ include [`WKB`] as a variant, because that is not zero-copy to parse.
#[derive(Debug, PartialEq)]
pub enum Geometry<'a, O: Offset> {
    Point(crate::scalar::Point<'a>),
    LineString(crate::scalar::LineString<'a, O>),
    Polygon(crate::scalar::Polygon<'a, O>),
    MultiPoint(crate::scalar::MultiPoint<'a, O>),
    MultiLineString(crate::scalar::MultiLineString<'a, O>),
    MultiPolygon(crate::scalar::MultiPolygon<'a, O>),
    Rect(crate::scalar::Rect<'a>),
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for Geometry<'a, O> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        match self {
            Geometry::Point(g) => geo::Geometry::Point(g.into()),
            Geometry::LineString(g) => geo::Geometry::LineString(g.into()),
            Geometry::Polygon(g) => geo::Geometry::Polygon(g.into()),
            Geometry::MultiPoint(g) => geo::Geometry::MultiPoint(g.into()),
            Geometry::MultiLineString(g) => geo::Geometry::MultiLineString(g.into()),
            Geometry::MultiPolygon(g) => geo::Geometry::MultiPolygon(g.into()),
            Geometry::Rect(g) => geo::Geometry::Rect(g.into()),
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
        match self {
            Geometry::Point(p) => GeometryType::Point(p),
            Geometry::LineString(p) => GeometryType::LineString(p),
            Geometry::Polygon(p) => GeometryType::Polygon(p),
            Geometry::MultiPoint(p) => GeometryType::MultiPoint(p),
            Geometry::MultiLineString(p) => GeometryType::MultiLineString(p),
            Geometry::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            // Geometry::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Geometry::Rect(p) => GeometryType::Rect(p),
        }
    }
}

impl<O: Offset> RTreeObject for Geometry<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match self {
            Geometry::Point(geom) => geom.envelope(),
            Geometry::LineString(geom) => geom.envelope(),
            Geometry::Polygon(geom) => geom.envelope(),
            Geometry::MultiPoint(geom) => geom.envelope(),
            Geometry::MultiLineString(geom) => geom.envelope(),
            Geometry::MultiPolygon(geom) => geom.envelope(),
            Geometry::Rect(geom) => geom.envelope(),
        }
    }
}

impl<O: Offset> From<Geometry<'_, O>> for geo::Geometry {
    fn from(value: Geometry<'_, O>) -> Self {
        match value {
            Geometry::Point(geom) => geom.into(),
            Geometry::LineString(geom) => geom.into(),
            Geometry::Polygon(geom) => geom.into(),
            Geometry::MultiPoint(geom) => geom.into(),
            Geometry::MultiLineString(geom) => geom.into(),
            Geometry::MultiPolygon(geom) => geom.into(),
            Geometry::Rect(geom) => geom.into(),
        }
    }
}
