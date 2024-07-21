use crate::algorithm::native::eq::geometry_eq;
use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::io::geo::geometry_to_geo;
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use arrow_array::OffsetSizeTrait;
use rstar::{RTreeObject, AABB};

/// A Geometry is an enum over the various underlying _zero copy_ GeoArrow scalar types.
///
/// Notably this does _not_ include [`WKB`] as a variant, because that is not zero-copy to parse.
#[derive(Debug)]
pub enum Geometry<'a, O: OffsetSizeTrait, const D: usize> {
    Point(crate::scalar::Point<'a, D>),
    LineString(crate::scalar::LineString<'a, O, D>),
    Polygon(crate::scalar::Polygon<'a, O, D>),
    MultiPoint(crate::scalar::MultiPoint<'a, O, D>),
    MultiLineString(crate::scalar::MultiLineString<'a, O, D>),
    MultiPolygon(crate::scalar::MultiPolygon<'a, O, D>),
    GeometryCollection(crate::scalar::GeometryCollection<'a, O, D>),
    Rect(crate::scalar::Rect<'a>),
}

impl<'a, O: OffsetSizeTrait, const D: usize> GeometryScalarTrait for Geometry<'a, O, D> {
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

impl<'a, O: OffsetSizeTrait, const D: usize> GeometryTrait<2> for Geometry<'a, O, D> {
    type T = f64;
    type Point<'b> = Point<'b, D> where Self: 'b;
    type LineString<'b> = LineString<'b, O, D> where Self: 'b;
    type Polygon<'b> = Polygon<'b, O, D> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'b, O, D> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'b, O, D> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'b, O, D> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'b, O, D> where Self: 'b;
    type Rect<'b> = Rect<'b> where Self: 'b;

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        Point<'_, D>,
        LineString<'_, O, D>,
        Polygon<'_, O, D>,
        MultiPoint<'_, O, D>,
        MultiLineString<'_, O, D>,
        MultiPolygon<'_, O, D>,
        GeometryCollection<'_, O, D>,
        Rect<'_>,
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

impl<'a, O: OffsetSizeTrait, const D: usize> GeometryTrait<2> for &'a Geometry<'a, O, D> {
    type T = f64;
    type Point<'b> = Point<'a, D> where Self: 'b;
    type LineString<'b> = LineString<'a, O, D> where Self: 'b;
    type Polygon<'b> = Polygon<'a, O, D> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'a, O, D> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'a, O, D> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'a, O, D> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'a, O, D> where Self: 'b;
    type Rect<'b> = Rect<'a> where Self: 'b;

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        'a,
        Point<'a, D>,
        LineString<'a, O, D>,
        Polygon<'a, O, D>,
        MultiPoint<'a, O, D>,
        MultiLineString<'a, O, D>,
        MultiPolygon<'a, O, D>,
        GeometryCollection<'a, O, D>,
        Rect<'a>,
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

impl<O: OffsetSizeTrait> RTreeObject for Geometry<'_, O, 2> {
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

impl<O: OffsetSizeTrait, const D: usize> From<Geometry<'_, O, D>> for geo::Geometry {
    fn from(value: Geometry<'_, O, D>) -> Self {
        geometry_to_geo(&value)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<&Geometry<'_, O, D>> for geo::Geometry {
    fn from(value: &Geometry<'_, O, D>) -> Self {
        geometry_to_geo(value)
    }
}

impl<O: OffsetSizeTrait, const D: usize, G: GeometryTrait<2, T = f64>> PartialEq<G>
    for Geometry<'_, O, D>
{
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
