use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use arrow_array::OffsetSizeTrait;
use rstar::{RTreeObject, AABB};

/// A Geometry is an enum over the various underlying _zero copy_ GeoArrow scalar types.
///
/// Notably this does _not_ include [`WKB`] as a variant, because that is not zero-copy to parse.
#[derive(Debug, PartialEq)]
pub enum Geometry<'a, O: OffsetSizeTrait> {
    Point(crate::scalar::Point<'a>),
    LineString(crate::scalar::LineString<'a, O>),
    Polygon(crate::scalar::Polygon<'a, O>),
    MultiPoint(crate::scalar::MultiPoint<'a, O>),
    MultiLineString(crate::scalar::MultiLineString<'a, O>),
    MultiPolygon(crate::scalar::MultiPolygon<'a, O>),
    Rect(crate::scalar::Rect<'a>),
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for Geometry<'a, O> {
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

impl<'a, O: OffsetSizeTrait> GeometryTrait for Geometry<'a, O> {
    type T = f64;
    type Point<'b> = Point<'b> where Self: 'b;
    type LineString<'b> = LineString<'b, O> where Self: 'b;
    type Polygon<'b> = Polygon<'b, O> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'b, O> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'b, O> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'b, O> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'b, O> where Self: 'b;
    type Rect<'b> = Rect<'b> where Self: 'b;

    // TODO: not 100% sure what this is
    #[allow(implied_bounds_entailment)]
    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        Point<'_>,
        LineString<'_, O>,
        Polygon<'_, O>,
        MultiPoint<'_, O>,
        MultiLineString<'_, O>,
        MultiPolygon<'_, O>,
        GeometryCollection<'_, O>,
        Rect<'_>,
    > {
        match self {
            Geometry::Point(p) => GeometryType::Point(p),
            Geometry::LineString(p) => GeometryType::LineString(p),
            Geometry::Polygon(p) => GeometryType::Polygon(p),
            Geometry::MultiPoint(p) => GeometryType::MultiPoint(p),
            Geometry::MultiLineString(p) => GeometryType::MultiLineString(p),
            Geometry::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            // Geometry::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            // Geometry::Rect(p) => GeometryType::Rect(p),
            _ => todo!(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> GeometryTrait for &'a Geometry<'a, O> {
    type T = f64;
    type Point<'b> = Point<'a> where Self: 'b;
    type LineString<'b> = LineString<'a, O> where Self: 'b;
    type Polygon<'b> = Polygon<'a, O> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'a, O> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'a, O> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'a, O> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'a, O> where Self: 'b;
    type Rect<'b> = Rect<'a> where Self: 'b;

    // TODO: not 100% sure what this is
    #[allow(implied_bounds_entailment)]
    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        'a,
        Point<'a>,
        LineString<'a, O>,
        Polygon<'a, O>,
        MultiPoint<'a, O>,
        MultiLineString<'a, O>,
        MultiPolygon<'a, O>,
        GeometryCollection<'a, O>,
        Rect<'a>,
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

impl<O: OffsetSizeTrait> RTreeObject for Geometry<'_, O> {
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

impl<O: OffsetSizeTrait> From<Geometry<'_, O>> for geo::Geometry {
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
