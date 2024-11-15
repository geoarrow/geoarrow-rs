use crate::algorithm::native::eq::geometry_eq;
use crate::scalar::*;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait, UnimplementedLine,
    UnimplementedTriangle,
};

#[derive(Clone, Debug)]
// TODO: come back to this in #449
#[allow(clippy::large_enum_variant)]
pub enum OwnedGeometry {
    Point(crate::scalar::OwnedPoint),
    LineString(crate::scalar::OwnedLineString),
    Polygon(crate::scalar::OwnedPolygon),
    MultiPoint(crate::scalar::OwnedMultiPoint),
    MultiLineString(crate::scalar::OwnedMultiLineString),
    MultiPolygon(crate::scalar::OwnedMultiPolygon),
    GeometryCollection(crate::scalar::OwnedGeometryCollection),
    Rect(crate::scalar::OwnedRect),
}

impl<'a> From<&'a OwnedGeometry> for Geometry<'a> {
    fn from(value: &'a OwnedGeometry) -> Self {
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

impl<'a> From<Geometry<'a>> for OwnedGeometry {
    fn from(value: Geometry<'a>) -> Self {
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

impl GeometryTrait for OwnedGeometry {
    type T = f64;
    type PointType<'b> = OwnedPoint where Self: 'b;
    type LineStringType<'b> = OwnedLineString where Self: 'b;
    type PolygonType<'b> = OwnedPolygon where Self: 'b;
    type MultiPointType<'b> = OwnedMultiPoint where Self: 'b;
    type MultiLineStringType<'b> = OwnedMultiLineString where Self: 'b;
    type MultiPolygonType<'b> = OwnedMultiPolygon where Self: 'b;
    type GeometryCollectionType<'b> = OwnedGeometryCollection where Self: 'b;
    type RectType<'b> = OwnedRect where Self: 'b;
    type TriangleType<'b> = UnimplementedTriangle<f64> where Self: 'b;
    type LineType<'b> = UnimplementedLine<f64> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Self::Point(p) => p.dim(),
            Self::LineString(p) => p.dim(),
            Self::Polygon(p) => p.dim(),
            Self::MultiPoint(p) => p.dim(),
            Self::MultiLineString(p) => p.dim(),
            Self::MultiPolygon(p) => p.dim(),
            Self::GeometryCollection(p) => p.dim(),
            Self::Rect(p) => p.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        OwnedPoint,
        OwnedLineString,
        OwnedPolygon,
        OwnedMultiPoint,
        OwnedMultiLineString,
        OwnedMultiPolygon,
        OwnedGeometryCollection,
        OwnedRect,
        UnimplementedTriangle<f64>,
        UnimplementedLine<f64>,
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

impl<G: GeometryTrait<T = f64>> PartialEq<G> for OwnedGeometry {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
