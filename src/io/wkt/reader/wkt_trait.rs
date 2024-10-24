use wkt::WktNum;

use crate::geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, UnimplementedLine,
    UnimplementedRect, UnimplementedTriangle,
};

impl<T: WktNum> CoordTrait for wkt::types::Coord<T> {
    type T = T;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match (self.z.is_some(), self.m.is_some()) {
            (false, false) => crate::geo_traits::Dimensions::Xy,
            (true, false) => crate::geo_traits::Dimensions::Xyz,
            (false, true) => crate::geo_traits::Dimensions::Xym,
            (true, true) => crate::geo_traits::Dimensions::Xyzm,
        }
    }

    fn x(&self) -> Self::T {
        self.x
    }

    fn y(&self) -> Self::T {
        self.y
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let has_z = self.z.is_some();
        let has_m = self.m.is_some();
        match n {
            0 => self.x,
            1 => self.y,
            2 => {
                if has_z {
                    self.z.unwrap()
                } else if has_m {
                    self.m.unwrap()
                } else {
                    panic!("n out of range")
                }
            }
            3 => {
                if has_z && has_m {
                    self.m.unwrap()
                } else {
                    panic!("n out of range")
                }
            }
            _ => panic!("n out of range"),
        }
    }
}

impl<'a, T: WktNum> CoordTrait for &'a wkt::types::Coord<T> {
    type T = T;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match (self.z.is_some(), self.m.is_some()) {
            (false, false) => crate::geo_traits::Dimensions::Xy,
            (true, false) => crate::geo_traits::Dimensions::Xyz,
            (false, true) => crate::geo_traits::Dimensions::Xym,
            (true, true) => crate::geo_traits::Dimensions::Xyzm,
        }
    }

    fn x(&self) -> Self::T {
        self.x
    }

    fn y(&self) -> Self::T {
        self.y
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let has_z = self.z.is_some();
        let has_m = self.m.is_some();
        match n {
            0 => self.x,
            1 => self.y,
            2 => {
                if has_z {
                    self.z.unwrap()
                } else if has_m {
                    self.m.unwrap()
                } else {
                    panic!("n out of range")
                }
            }
            3 => {
                if has_z && has_m {
                    self.m.unwrap()
                } else {
                    panic!("n out of range")
                }
            }
            _ => panic!("n out of range"),
        }
    }
}

impl<T: WktNum> PointTrait for wkt::types::Point<T> {
    type T = T;
    type CoordType<'a> = &'a wkt::types::Coord<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.0.as_ref().unwrap().dim()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        self.0.as_ref()
    }
}

impl<'a, T: WktNum> PointTrait for &'a wkt::types::Point<T> {
    type T = T;
    type CoordType<'b> = &'a wkt::types::Coord<T> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.0.as_ref().unwrap().dim()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        self.0.as_ref()
    }
}

impl<T: WktNum> LineStringTrait for wkt::types::LineString<T> {
    type T = T;
    type CoordType<'a> = &'a wkt::types::Coord<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        &self.0[i]
    }
}

impl<'a, T: WktNum> LineStringTrait for &'a wkt::types::LineString<T> {
    type T = T;
    type CoordType<'b> = &'b wkt::types::Coord<T> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        &self.0[i]
    }
}

impl<T: WktNum> PolygonTrait for wkt::types::Polygon<T> {
    type T = T;
    type RingType<'a> = &'a wkt::types::LineString<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        self.0.first()
    }

    fn num_interiors(&self) -> usize {
        self.0.len().saturating_sub(1)
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        &self.0[i + 1]
    }
}

impl<'a, T: WktNum> PolygonTrait for &'a wkt::types::Polygon<T> {
    type T = T;
    type RingType<'b> = &'b wkt::types::LineString<T> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        self.0.first()
    }

    fn num_interiors(&self) -> usize {
        self.0.len().saturating_sub(1)
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        &self.0[i + 1]
    }
}

impl<T: WktNum> MultiPointTrait for wkt::types::MultiPoint<T> {
    type T = T;
    type PointType<'a> = &'a wkt::types::Point<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn num_points(&self) -> usize {
        self.0.len()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        &self.0[i]
    }
}

impl<T: WktNum> MultiLineStringTrait for wkt::types::MultiLineString<T> {
    type T = T;
    type LineStringType<'a> = &'a wkt::types::LineString<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn num_line_strings(&self) -> usize {
        self.0.len()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        &self.0[i]
    }
}

impl<T: WktNum> MultiPolygonTrait for wkt::types::MultiPolygon<T> {
    type T = T;
    type PolygonType<'a> = &'a wkt::types::Polygon<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        &self.0[i]
    }
}

impl<T: WktNum> GeometryTrait for wkt::Wkt<T> {
    type T = T;
    type PointType<'b> = wkt::types::Point<T> where Self: 'b;
    type LineStringType<'b> = wkt::types::LineString<T> where Self: 'b;
    type PolygonType<'b> = wkt::types::Polygon<T> where Self: 'b;
    type MultiPointType<'b> = wkt::types::MultiPoint<T> where Self: 'b;
    type MultiLineStringType<'b> = wkt::types::MultiLineString<T> where Self: 'b;
    type MultiPolygonType<'b> = wkt::types::MultiPolygon<T> where Self: 'b;
    type GeometryCollectionType<'b> = wkt::types::GeometryCollection<T> where Self: 'b;
    type RectType<'b> = UnimplementedRect<T> where Self: 'b;
    type TriangleType<'b> = UnimplementedTriangle<T> where Self: 'b;
    type LineType<'b> = UnimplementedLine<T> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        use wkt::Wkt::*;
        match self {
            Point(geom) => geom.dim(),
            LineString(geom) => geom.dim(),
            Polygon(geom) => geom.dim(),
            MultiPoint(geom) => geom.dim(),
            MultiLineString(geom) => geom.dim(),
            MultiPolygon(geom) => geom.dim(),
            GeometryCollection(geom) => geom.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        wkt::types::Point<T>,
        wkt::types::LineString<T>,
        wkt::types::Polygon<T>,
        wkt::types::MultiPoint<T>,
        wkt::types::MultiLineString<T>,
        wkt::types::MultiPolygon<T>,
        wkt::types::GeometryCollection<T>,
        UnimplementedRect<T>,
        UnimplementedTriangle<T>,
        UnimplementedLine<T>,
    > {
        match self {
            wkt::Wkt::Point(geom) => crate::geo_traits::GeometryType::Point(geom),
            wkt::Wkt::LineString(geom) => crate::geo_traits::GeometryType::LineString(geom),
            wkt::Wkt::Polygon(geom) => crate::geo_traits::GeometryType::Polygon(geom),
            wkt::Wkt::MultiPoint(geom) => crate::geo_traits::GeometryType::MultiPoint(geom),
            wkt::Wkt::MultiLineString(geom) => {
                crate::geo_traits::GeometryType::MultiLineString(geom)
            }
            wkt::Wkt::MultiPolygon(geom) => crate::geo_traits::GeometryType::MultiPolygon(geom),
            wkt::Wkt::GeometryCollection(geom) => {
                crate::geo_traits::GeometryType::GeometryCollection(geom)
            }
        }
    }
}

impl<'a, T: WktNum> GeometryTrait for &'a wkt::Wkt<T> {
    type T = T;
    type PointType<'b> = wkt::types::Point<T> where Self: 'b;
    type LineStringType<'b> = wkt::types::LineString<T> where Self: 'b;
    type PolygonType<'b> = wkt::types::Polygon<T> where Self: 'b;
    type MultiPointType<'b> = wkt::types::MultiPoint<T> where Self: 'b;
    type MultiLineStringType<'b> = wkt::types::MultiLineString<T> where Self: 'b;
    type MultiPolygonType<'b> = wkt::types::MultiPolygon<T> where Self: 'b;
    type GeometryCollectionType<'b> = wkt::types::GeometryCollection<T> where Self: 'b;
    type RectType<'b> = UnimplementedRect<T> where Self: 'b;
    type TriangleType<'b> = UnimplementedTriangle<T> where Self: 'b;
    type LineType<'b> = UnimplementedLine<T> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        use wkt::Wkt::*;
        match self {
            Point(geom) => geom.dim(),
            LineString(geom) => geom.dim(),
            Polygon(geom) => geom.dim(),
            MultiPoint(geom) => geom.dim(),
            MultiLineString(geom) => geom.dim(),
            MultiPolygon(geom) => geom.dim(),
            GeometryCollection(geom) => geom.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        wkt::types::Point<T>,
        wkt::types::LineString<T>,
        wkt::types::Polygon<T>,
        wkt::types::MultiPoint<T>,
        wkt::types::MultiLineString<T>,
        wkt::types::MultiPolygon<T>,
        wkt::types::GeometryCollection<T>,
        UnimplementedRect<T>,
        UnimplementedTriangle<T>,
        UnimplementedLine<T>,
    > {
        match self {
            wkt::Wkt::Point(geom) => crate::geo_traits::GeometryType::Point(geom),
            wkt::Wkt::LineString(geom) => crate::geo_traits::GeometryType::LineString(geom),
            wkt::Wkt::Polygon(geom) => crate::geo_traits::GeometryType::Polygon(geom),
            wkt::Wkt::MultiPoint(geom) => crate::geo_traits::GeometryType::MultiPoint(geom),
            wkt::Wkt::MultiLineString(geom) => {
                crate::geo_traits::GeometryType::MultiLineString(geom)
            }
            wkt::Wkt::MultiPolygon(geom) => crate::geo_traits::GeometryType::MultiPolygon(geom),
            wkt::Wkt::GeometryCollection(geom) => {
                crate::geo_traits::GeometryType::GeometryCollection(geom)
            }
        }
    }
}

impl<T: WktNum> GeometryCollectionTrait for wkt::types::GeometryCollection<T> {
    type T = T;
    type GeometryType<'a> = &'a wkt::Wkt<T> where Self: 'a;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        if self.0.is_empty() {
            crate::geo_traits::Dimensions::Xy
        } else {
            self.0[0].dim()
        }
    }

    fn num_geometries(&self) -> usize {
        self.0.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        &self.0[i]
    }
}
