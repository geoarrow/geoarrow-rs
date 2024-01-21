use arrow_array::OffsetSizeTrait;

use crate::algorithm::native::eq::geometry_eq;
use crate::geo_traits::{GeometryTrait, GeometryType};
use crate::scalar::*;

#[derive(Debug)]
// TODO: come back to this in #449
#[allow(clippy::large_enum_variant)]
pub enum OwnedGeometry<O: OffsetSizeTrait> {
    Point(crate::scalar::OwnedPoint),
    LineString(crate::scalar::OwnedLineString<O>),
    Polygon(crate::scalar::OwnedPolygon<O>),
    MultiPoint(crate::scalar::OwnedMultiPoint<O>),
    MultiLineString(crate::scalar::OwnedMultiLineString<O>),
    MultiPolygon(crate::scalar::OwnedMultiPolygon<O>),
    GeometryCollection(crate::scalar::OwnedGeometryCollection<O>),
    Rect(crate::scalar::OwnedRect),
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedGeometry<O>> for Geometry<'a, O> {
    fn from(value: &'a OwnedGeometry<O>) -> Self {
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

impl<'a, O: OffsetSizeTrait> From<&'a OwnedGeometry<O>> for geo::Geometry {
    fn from(value: &'a OwnedGeometry<O>) -> Self {
        let geom = Geometry::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<Geometry<'a, O>> for OwnedGeometry<O> {
    fn from(value: Geometry<'a, O>) -> Self {
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

impl<O: OffsetSizeTrait> GeometryTrait for OwnedGeometry<O> {
    type T = f64;
    type Point<'b> = OwnedPoint where Self: 'b;
    type LineString<'b> = OwnedLineString< O> where Self: 'b;
    type Polygon<'b> = OwnedPolygon< O> where Self: 'b;
    type MultiPoint<'b> = OwnedMultiPoint< O> where Self: 'b;
    type MultiLineString<'b> = OwnedMultiLineString< O> where Self: 'b;
    type MultiPolygon<'b> = OwnedMultiPolygon< O> where Self: 'b;
    type GeometryCollection<'b> = OwnedGeometryCollection< O> where Self: 'b;
    type Rect<'b> = OwnedRect where Self: 'b;

    // TODO: not 100% sure what this is
    #[allow(implied_bounds_entailment)]
    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        OwnedPoint,
        OwnedLineString<O>,
        OwnedPolygon<O>,
        OwnedMultiPoint<O>,
        OwnedMultiLineString<O>,
        OwnedMultiPolygon<O>,
        OwnedGeometryCollection<O>,
        OwnedRect,
    > {
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

impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> PartialEq<G> for OwnedGeometry<O> {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
