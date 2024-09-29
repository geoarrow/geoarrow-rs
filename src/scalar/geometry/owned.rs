use crate::algorithm::native::eq::geometry_eq;
use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::scalar::*;

#[derive(Clone, Debug)]
// TODO: come back to this in #449
#[allow(clippy::large_enum_variant)]
pub enum OwnedGeometry<const D: usize> {
    Point(crate::scalar::OwnedPoint<D>),
    LineString(crate::scalar::OwnedLineString<D>),
    Polygon(crate::scalar::OwnedPolygon<D>),
    MultiPoint(crate::scalar::OwnedMultiPoint<D>),
    MultiLineString(crate::scalar::OwnedMultiLineString<D>),
    MultiPolygon(crate::scalar::OwnedMultiPolygon<D>),
    GeometryCollection(crate::scalar::OwnedGeometryCollection<D>),
    Rect(crate::scalar::OwnedRect<D>),
}

impl<'a, const D: usize> From<&'a OwnedGeometry<D>> for Geometry<'a, D> {
    fn from(value: &'a OwnedGeometry<D>) -> Self {
        use OwnedGeometry::*;
        match value {
            Point(geom) => Geometry::Point(geom.into()),
            LineString(geom) => Geometry::LineString(geom.into()),
            Polygon(geom) => Geometry::Polygon(geom.into()),
            MultiPoint(geom) => Geometry::MultiPoint(geom.into()),
            MultiLineString(geom) => Geometry::MultiLineString(geom.into()),
            MultiPolygon(geom) => Geometry::MultiPolygon(geom.into()),
            GeometryCollection(geom) => Geometry::GeometryCollection(geom.into()),
            Rect(geom) => Geometry::Rect(geom.into()),
        }
    }
}

impl<'a> From<&'a OwnedGeometry<2>> for geo::Geometry {
    fn from(value: &'a OwnedGeometry<2>) -> Self {
        let geom = Geometry::from(value);
        geom.into()
    }
}

impl<'a, const D: usize> From<Geometry<'a, D>> for OwnedGeometry<D> {
    fn from(value: Geometry<'a, D>) -> Self {
        use OwnedGeometry::*;
        match value {
            Geometry::Point(geom) => Point(geom.into()),
            Geometry::LineString(geom) => LineString(geom.into()),
            Geometry::Polygon(geom) => Polygon(geom.into()),
            Geometry::MultiPoint(geom) => MultiPoint(geom.into()),
            Geometry::MultiLineString(geom) => MultiLineString(geom.into()),
            Geometry::MultiPolygon(geom) => MultiPolygon(geom.into()),
            Geometry::GeometryCollection(geom) => GeometryCollection(geom.into()),
            Geometry::Rect(geom) => Rect(geom.into()),
        }
    }
}

// impl<O: OffsetSizeTrait> From<OwnedGeometry<O>> for MixedGeometryArray<O> {
//     fn from(value: OwnedGeometry<O>) -> Self {
//         match value {
//         }
//     }
// }

impl<const D: usize> GeometryTrait for OwnedGeometry<D> {
    type T = f64;
    type Point<'b> = OwnedPoint<D> where Self: 'b;
    type LineString<'b> = OwnedLineString<D> where Self: 'b;
    type Polygon<'b> = OwnedPolygon<D> where Self: 'b;
    type MultiPoint<'b> = OwnedMultiPoint<D> where Self: 'b;
    type MultiLineString<'b> = OwnedMultiLineString<D> where Self: 'b;
    type MultiPolygon<'b> = OwnedMultiPolygon<D> where Self: 'b;
    type GeometryCollection<'b> = OwnedGeometryCollection<D> where Self: 'b;
    type Rect<'b> = OwnedRect<D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn as_type(&self) -> crate::geo_traits::GeometryType<'_, OwnedPoint<D>, OwnedLineString<D>, OwnedPolygon<D>, OwnedMultiPoint<D>, OwnedMultiLineString<D>, OwnedMultiPolygon<D>, OwnedGeometryCollection<D>, OwnedRect<D>> {
        match self {
            Self::Point(p) => GeometryType::Point(p),
            Self::LineString(p) => GeometryType::LineString(p),
            Self::Polygon(p) => GeometryType::Polygon(p),
            Self::MultiPoint(p) => GeometryType::MultiPoint(p),
            Self::MultiLineString(p) => GeometryType::MultiLineString(p),
            Self::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            Self::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Self::Rect(p) => GeometryType::Rect(p),
        }
    }
}

impl<G: GeometryTrait<T = f64>> PartialEq<G> for OwnedGeometry<2> {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
