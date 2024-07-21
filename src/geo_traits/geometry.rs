use geo::{
    CoordNum, Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon, Rect,
};

use super::{
    GeometryCollectionTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

/// A trait for accessing data from a generic Geometry.
#[allow(clippy::type_complexity)]
pub trait GeometryTrait<const DIM: usize> {
    type T: CoordNum;
    type Point<'a>: 'a + PointTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type LineString<'a>: 'a + LineStringTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type Polygon<'a>: 'a + PolygonTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type MultiPoint<'a>: 'a + MultiPointTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type MultiLineString<'a>: 'a + MultiLineStringTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type MultiPolygon<'a>: 'a + MultiPolygonTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type GeometryCollection<'a>: 'a + GeometryCollectionTrait<DIM, T = Self::T>
    where
        Self: 'a;
    type Rect<'a>: 'a + RectTrait<DIM, T = Self::T>
    where
        Self: 'a;

    /// Native dimension of the coordinate tuple
    fn dim(&self) -> usize {
        DIM
    }

    fn as_type(
        &self,
    ) -> GeometryType<
        '_,
        DIM,
        Self::Point<'_>,
        Self::LineString<'_>,
        Self::Polygon<'_>,
        Self::MultiPoint<'_>,
        Self::MultiLineString<'_>,
        Self::MultiPolygon<'_>,
        Self::GeometryCollection<'_>,
        Self::Rect<'_>,
    >;
}

/// An enumeration of all geometry types that can be contained inside a [GeometryTrait<2>]. This is
/// used for extracting concrete geometry types out of a [GeometryTrait<2>].
#[derive(Debug)]
pub enum GeometryType<'a, const DIM: usize, P, L, Y, MP, ML, MY, GC, R>
where
    P: PointTrait<DIM>,
    L: LineStringTrait<DIM>,
    Y: PolygonTrait<DIM>,
    MP: MultiPointTrait<DIM>,
    ML: MultiLineStringTrait<DIM>,
    MY: MultiPolygonTrait<DIM>,
    GC: GeometryCollectionTrait<DIM>,
    R: RectTrait<DIM>,
{
    Point(&'a P),
    LineString(&'a L),
    Polygon(&'a Y),
    MultiPoint(&'a MP),
    MultiLineString(&'a ML),
    MultiPolygon(&'a MY),
    GeometryCollection(&'a GC),
    Rect(&'a R),
}

impl<'a, T: CoordNum + 'a> GeometryTrait<2> for Geometry<T> {
    type T = T;
    type Point<'b> = Point<Self::T> where Self: 'b;
    type LineString<'b> = LineString<Self::T> where Self: 'b;
    type Polygon<'b> = Polygon<Self::T> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<Self::T> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<Self::T> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<Self::T> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<Self::T> where Self: 'b;
    type Rect<'b> = Rect<Self::T> where Self: 'b;

    fn as_type(
        &self,
    ) -> GeometryType<
        '_,
        2,
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

impl<'a, T: CoordNum + 'a> GeometryTrait<2> for &'a Geometry<T> {
    type T = T;
    type Point<'b> = Point<Self::T> where Self: 'b;
    type LineString<'b> = LineString<Self::T> where Self: 'b;
    type Polygon<'b> = Polygon<Self::T> where Self: 'b;
    type MultiPoint<'b> = MultiPoint<Self::T> where Self: 'b;
    type MultiLineString<'b> = MultiLineString<Self::T> where Self: 'b;
    type MultiPolygon<'b> = MultiPolygon<Self::T> where Self: 'b;
    type GeometryCollection<'b> = GeometryCollection<Self::T> where Self: 'b;
    type Rect<'b> = Rect<Self::T> where Self: 'b;

    fn as_type(
        &self,
    ) -> GeometryType<
        '_,
        2,
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
