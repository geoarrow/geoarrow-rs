use crate::datatypes::Dimension;
use crate::geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, UnimplementedLine,
    UnimplementedRect, UnimplementedTriangle,
};

#[derive(Debug, Clone)]
pub(super) struct Coord<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,
    /// The coordinate offset
    ///
    /// Note each coord_offset points to an xy coordinate pair, and must be multiplied by 2 to get
    /// the buffer coord_offset
    coord_offset: usize,
}

impl<'a> CoordTrait for Coord<'a> {
    type T = f64;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.geom.xy().unwrap().get(self.coord_offset * 2),
            1 => self.geom.xy().unwrap().get((self.coord_offset * 2) + 1),
            2 => self.geom.z().unwrap().get(self.coord_offset),
            _ => panic!("Unexpected dim {n}"),
        }
    }

    fn x(&self) -> Self::T {
        self.geom.xy().unwrap().get(self.coord_offset * 2)
    }

    fn y(&self) -> Self::T {
        self.geom.xy().unwrap().get((self.coord_offset * 2) + 1)
    }
}

#[derive(Debug, Clone)]
pub(super) struct Point<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,
    /// The coordinate offset
    ///
    /// Note each coord_offset points to an xy coordinate pair, and must be multiplied by 2 to get
    /// the buffer coord_offset
    coord_offset: usize,
}

impl<'a> Point<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        Self {
            geom,
            dim,
            coord_offset: 0,
        }
    }
}

impl<'a> PointTrait for Point<'a> {
    type T = f64;
    type CoordType<'b> = Coord<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        // FlatGeobuf doesn't support empty geometries
        Some(Coord {
            geom: self.geom,
            dim: self.dim,
            coord_offset: self.coord_offset,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct LineString<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,

    /// This coord_offset will be non-zero when the LineString is a reference onto an external geometry,
    /// e.g. a Polygon
    coord_offset: usize,

    /// This length cannot be inferred from the underlying buffer when this LineString is a
    /// reference on e.g. a Polygon
    length: usize,
}

impl<'a> LineString<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        let length = geom.xy().unwrap().len() / 2;
        Self {
            geom,
            dim,
            coord_offset: 0,
            length,
        }
    }
}

impl<'a> LineStringTrait for LineString<'a> {
    type T = f64;
    type CoordType<'b> = Coord<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_coords(&self) -> usize {
        self.length
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        Coord {
            geom: self.geom,
            dim: self.dim,
            coord_offset: self.coord_offset + i,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Polygon<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,
}

impl<'a> Polygon<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        Self { geom, dim }
    }
}

impl<'a> PolygonTrait for Polygon<'a> {
    type T = f64;
    type RingType<'b> = LineString<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_interiors(&self) -> usize {
        if let Some(ends) = self.geom.ends() {
            ends.len() - 1
        } else {
            0
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        if let Some(ends) = self.geom.ends() {
            let exterior_end = ends.get(0);
            Some(LineString {
                geom: self.geom,
                dim: self.dim,
                coord_offset: 0,
                length: exterior_end.try_into().unwrap(),
            })
        } else {
            Some(LineString::new(self.geom, self.dim))
        }
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        let ends = self.geom.ends().unwrap();
        let start = ends.get(i);
        let end = ends.get(i + 1);
        LineString {
            geom: self.geom,
            dim: self.dim,
            coord_offset: start.try_into().unwrap(),
            length: (end - start).try_into().unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MultiPoint<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,

    /// This coord_offset will be non-zero when the MultiPoint is a reference onto an external geometry,
    /// e.g. a GeometryCollection
    coord_offset: usize,

    /// This length is not inferred from the underlying buffer because this MultiPoint could be a
    /// reference on e.g. a GeometryCollection
    length: usize,
}

impl<'a> MultiPoint<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        let length = geom.xy().unwrap().len() / 2;
        Self {
            geom,
            dim,
            coord_offset: 0,
            length,
        }
    }
}

impl<'a> MultiPointTrait for MultiPoint<'a> {
    type T = f64;
    type PointType<'b> = Point<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_points(&self) -> usize {
        self.length
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        Point {
            geom: self.geom,
            dim: self.dim,
            coord_offset: self.coord_offset + i,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MultiLineString<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,
}

impl<'a> MultiLineString<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        Self { geom, dim }
    }
}

impl<'a> MultiLineStringTrait for MultiLineString<'a> {
    type T = f64;
    type LineStringType<'b> = LineString<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_line_strings(&self) -> usize {
        if let Some(ends) = self.geom.ends() {
            ends.len()
        } else {
            1
        }
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        if let Some(ends) = self.geom.ends() {
            let start = if i == 0 { 0 } else { ends.get(i - 1) };
            let end = ends.get(i);
            LineString {
                geom: self.geom,
                dim: self.dim,
                coord_offset: start.try_into().unwrap(),
                length: (end - start).try_into().unwrap(),
            }
        } else {
            assert_eq!(i, 0);
            LineString::new(self.geom, self.dim)
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MultiPolygon<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,
}

impl<'a> MultiPolygon<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        Self { geom, dim }
    }
}

impl<'a> MultiPolygonTrait for MultiPolygon<'a> {
    type T = f64;
    type PolygonType<'b> = Polygon<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_polygons(&self) -> usize {
        self.geom.parts().unwrap().len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        Polygon {
            geom: self.geom.parts().unwrap().get(i),
            dim: self.dim,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum Geometry<'a> {
    Point(Point<'a>),
    LineString(LineString<'a>),
    Polygon(Polygon<'a>),
    MultiPoint(MultiPoint<'a>),
    MultiLineString(MultiLineString<'a>),
    MultiPolygon(MultiPolygon<'a>),
    #[allow(clippy::enum_variant_names)]
    GeometryCollection(GeometryCollection<'a>),
}

impl<'a> Geometry<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        match geom.type_() {
            flatgeobuf::GeometryType::Point => Self::Point(Point::new(geom, dim)),
            flatgeobuf::GeometryType::LineString => Self::LineString(LineString::new(geom, dim)),
            flatgeobuf::GeometryType::Polygon => Self::Polygon(Polygon::new(geom, dim)),
            flatgeobuf::GeometryType::MultiPoint => Self::MultiPoint(MultiPoint::new(geom, dim)),
            flatgeobuf::GeometryType::MultiLineString => {
                Self::MultiLineString(MultiLineString::new(geom, dim))
            }
            flatgeobuf::GeometryType::MultiPolygon => {
                Self::MultiPolygon(MultiPolygon::new(geom, dim))
            }
            flatgeobuf::GeometryType::GeometryCollection => {
                Self::GeometryCollection(GeometryCollection::new(geom, dim))
            }
            t => panic!("Unexpected type {t:?}"),
        }
    }
}

impl<'a> From<Point<'a>> for Geometry<'a> {
    fn from(value: Point<'a>) -> Self {
        Self::Point(value)
    }
}

impl<'a> From<LineString<'a>> for Geometry<'a> {
    fn from(value: LineString<'a>) -> Self {
        Self::LineString(value)
    }
}

impl<'a> From<Polygon<'a>> for Geometry<'a> {
    fn from(value: Polygon<'a>) -> Self {
        Self::Polygon(value)
    }
}

impl<'a> From<MultiPoint<'a>> for Geometry<'a> {
    fn from(value: MultiPoint<'a>) -> Self {
        Self::MultiPoint(value)
    }
}

impl<'a> From<MultiLineString<'a>> for Geometry<'a> {
    fn from(value: MultiLineString<'a>) -> Self {
        Self::MultiLineString(value)
    }
}

impl<'a> From<MultiPolygon<'a>> for Geometry<'a> {
    fn from(value: MultiPolygon<'a>) -> Self {
        Self::MultiPolygon(value)
    }
}

impl<'a> From<GeometryCollection<'a>> for Geometry<'a> {
    fn from(value: GeometryCollection<'a>) -> Self {
        Self::GeometryCollection(value)
    }
}

impl<'a> GeometryTrait for Geometry<'a> {
    type T = f64;
    type PointType<'b> = Point<'a> where Self: 'b;
    type LineStringType<'b> = LineString<'a> where Self: 'b;
    type PolygonType<'b> = Polygon<'a> where Self: 'b;
    type MultiPointType<'b> = MultiPoint<'a> where Self: 'b;
    type MultiLineStringType<'b> = MultiLineString<'a> where Self: 'b;
    type MultiPolygonType<'b> = MultiPolygon<'a> where Self: 'b;
    type GeometryCollectionType<'b> = GeometryCollection<'a> where Self: 'b;
    type RectType<'b> = UnimplementedRect<f64> where Self: 'b;
    type TriangleType<'b> = UnimplementedTriangle<f64> where Self: 'b;
    type LineType<'b> = UnimplementedLine<f64> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self {
            Self::Point(g) => PointTrait::dim(g),
            Self::LineString(g) => g.dim(),
            Self::Polygon(g) => g.dim(),
            Self::MultiPoint(g) => g.dim(),
            Self::MultiLineString(g) => g.dim(),
            Self::MultiPolygon(g) => g.dim(),
            Self::GeometryCollection(g) => g.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        Point<'a>,
        LineString<'a>,
        Polygon<'a>,
        MultiPoint<'a>,
        MultiLineString<'a>,
        MultiPolygon<'a>,
        GeometryCollection<'a>,
        UnimplementedRect<f64>,
        UnimplementedTriangle<f64>,
        UnimplementedLine<f64>,
    > {
        match self {
            Self::Point(pt) => crate::geo_traits::GeometryType::Point(pt),
            Self::LineString(pt) => crate::geo_traits::GeometryType::LineString(pt),
            Self::Polygon(pt) => crate::geo_traits::GeometryType::Polygon(pt),
            Self::MultiPoint(pt) => crate::geo_traits::GeometryType::MultiPoint(pt),
            Self::MultiLineString(pt) => crate::geo_traits::GeometryType::MultiLineString(pt),
            Self::MultiPolygon(pt) => crate::geo_traits::GeometryType::MultiPolygon(pt),
            Self::GeometryCollection(pt) => crate::geo_traits::GeometryType::GeometryCollection(pt),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct GeometryCollection<'a> {
    geom: flatgeobuf::Geometry<'a>,
    dim: Dimension,
}

impl<'a> GeometryCollection<'a> {
    pub(super) fn new(geom: flatgeobuf::Geometry<'a>, dim: Dimension) -> Self {
        Self { geom, dim }
    }
}

impl<'a> GeometryCollectionTrait for GeometryCollection<'a> {
    type T = f64;
    type GeometryType<'b> = Geometry<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_geometries(&self) -> usize {
        let parts = self.geom.parts().unwrap();
        parts.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        Geometry::new(self.geom.parts().unwrap().get(i), self.dim)
    }
}
