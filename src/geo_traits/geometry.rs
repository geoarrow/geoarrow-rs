use geo::{
    CoordNum, Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon, Rect,
};

use super::{
    GeometryCollectionTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

#[allow(clippy::type_complexity)]
pub trait GeometryTrait<'a, 'b: 'a> {
    type T: CoordNum;
    type Point: 'b + PointTrait<T = Self::T>;
    type LineString: 'b + LineStringTrait<'b, T = Self::T>;
    type Polygon: 'b + PolygonTrait<'b, T = Self::T>;
    type MultiPoint: 'b + MultiPointTrait<'b, T = Self::T>;
    type MultiLineString: 'b + MultiLineStringTrait<'b, T = Self::T>;
    type MultiPolygon: 'b + MultiPolygonTrait<'b, T = Self::T>;
    type GeometryCollection: 'b + GeometryCollectionTrait<'a, 'b, T = Self::T>;
    type Rect: 'b + RectTrait<'b, T = Self::T>;

    fn as_type(
        & self,
    ) -> GeometryType<
        'a, 'b,
        Self::Point,
        Self::LineString,
        Self::Polygon,
        Self::MultiPoint,
        Self::MultiLineString,
        Self::MultiPolygon,
        Self::GeometryCollection,
        Self::Rect,
    >;
}

#[derive(Debug)]
pub enum GeometryType<'a, 'b: 'a, P, L, Y, MP, ML, MY, GC, R>
where
    P: PointTrait,
    L: LineStringTrait<'b>,
    Y: PolygonTrait<'b>,
    MP: MultiPointTrait<'b>,
    ML: MultiLineStringTrait<'b>,
    MY: MultiPolygonTrait<'b>,
    GC: GeometryCollectionTrait<'a, 'b>,
    R: RectTrait<'b>,
{
    Point(&'b P),
    LineString(&'b L),
    Polygon(&'b Y),
    MultiPoint(&'b MP),
    MultiLineString(&'b ML),
    MultiPolygon(&'b MY),
    GeometryCollection(&'a GC),
    Rect(&'b R),
}

impl<'a, 'b: 'a, T: CoordNum + 'a + 'b> GeometryTrait<'a, 'b> for Geometry<T> {
    type T = T;
    type Point = Point<Self::T>;
    type LineString = LineString<Self::T>;
    type Polygon = Polygon<Self::T>;
    type MultiPoint = MultiPoint<Self::T>;
    type MultiLineString = MultiLineString<Self::T>;
    type MultiPolygon = MultiPolygon<Self::T>;
    type GeometryCollection = GeometryCollection<Self::T>;
    type Rect = Rect<Self::T>;

    fn as_type(
        & self,
    ) -> GeometryType<
        'a, 'b,
        Point<T>,
        LineString<T>,
        Polygon<T>,
        MultiPoint<T>,
        MultiLineString<T>,
        MultiPolygon<T>,
        GeometryCollection<T>,
        Rect<T>,
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
            _ => todo!(),
        }
    }
}
