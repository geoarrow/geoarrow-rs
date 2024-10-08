use crate::algorithm::native::eq::geometry_eq;
use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::io::geo::geometry_to_geo;
use crate::scalar::*;
use crate::trait_::NativeScalar;
use rstar::{RTreeObject, AABB};

/// A Geometry is an enum over the various underlying _zero copy_ GeoArrow scalar types.
#[derive(Debug)]
pub enum Geometry<'a, const D: usize> {
    Point(crate::scalar::Point<'a, D>),
    LineString(crate::scalar::LineString<'a, D>),
    Polygon(crate::scalar::Polygon<'a, D>),
    MultiPoint(crate::scalar::MultiPoint<'a, D>),
    MultiLineString(crate::scalar::MultiLineString<'a, D>),
    MultiPolygon(crate::scalar::MultiPolygon<'a, D>),
    GeometryCollection(crate::scalar::GeometryCollection<'a, D>),
    Rect(crate::scalar::Rect<'a, D>),
}

impl<'a, const D: usize> NativeScalar for Geometry<'a, D> {
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

impl<'a, const D: usize> GeometryTrait for Geometry<'a, D> {
    type T = f64;
    type Point<'b> = Point<'b, D> where Self: 'b;
    type LineString<'b> = LineString<'b, D> where Self: 'b;
    type Polygon<'b> = Polygon<'b, D> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'b, D> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'b, D> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'b, D> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'b, D> where Self: 'b;
    type Rect<'b> = Rect<'b, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        Point<'_, D>,
        LineString<'_, D>,
        Polygon<'_, D>,
        MultiPoint<'_, D>,
        MultiLineString<'_, D>,
        MultiPolygon<'_, D>,
        GeometryCollection<'_, D>,
        Rect<'_, D>,
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

impl<'a, const D: usize> GeometryTrait for &'a Geometry<'a, D> {
    type T = f64;
    type Point<'b> = Point<'a, D> where Self: 'b;
    type LineString<'b> = LineString<'a, D> where Self: 'b;
    type Polygon<'b> = Polygon<'a, D> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<'a, D> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<'a, D> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<'a, D> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<'a, D> where Self: 'b;
    type Rect<'b> = Rect<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        'a,
        Point<'a, D>,
        LineString<'a, D>,
        Polygon<'a, D>,
        MultiPoint<'a, D>,
        MultiLineString<'a, D>,
        MultiPolygon<'a, D>,
        GeometryCollection<'a, D>,
        Rect<'a, D>,
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

impl RTreeObject for Geometry<'_, 2> {
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

impl<const D: usize> From<Geometry<'_, D>> for geo::Geometry {
    fn from(value: Geometry<'_, D>) -> Self {
        geometry_to_geo(&value)
    }
}

impl<const D: usize> From<&Geometry<'_, D>> for geo::Geometry {
    fn from(value: &Geometry<'_, D>) -> Self {
        geometry_to_geo(value)
    }
}

impl<const D: usize, G: GeometryTrait<T = f64>> PartialEq<G> for Geometry<'_, D> {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
