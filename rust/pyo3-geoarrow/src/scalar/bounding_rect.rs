use std::ops::Add;

use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait, UnimplementedGeometryCollection, UnimplementedLine, UnimplementedLineString,
    UnimplementedMultiLineString, UnimplementedMultiPoint, UnimplementedMultiPolygon,
    UnimplementedPoint, UnimplementedPolygon, UnimplementedTriangle,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;

#[derive(Debug, Clone, Copy)]
pub struct BoundingRect {
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
}

impl BoundingRect {
    /// New
    pub fn new() -> Self {
        BoundingRect {
            minx: f64::INFINITY,
            miny: f64::INFINITY,
            maxx: -f64::INFINITY,
            maxy: -f64::INFINITY,
        }
    }

    pub fn minx(&self) -> f64 {
        self.minx
    }

    pub fn miny(&self) -> f64 {
        self.miny
    }

    pub fn maxx(&self) -> f64 {
        self.maxx
    }

    pub fn maxy(&self) -> f64 {
        self.maxy
    }

    pub fn add_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        let x = coord.x();
        let y = coord.y();

        if x < self.minx {
            self.minx = x;
        }
        if y < self.miny {
            self.miny = y;
        }
        if x > self.maxx {
            self.maxx = x;
        }
        if y > self.maxy {
            self.maxy = y;
        }
    }

    pub fn add_point(&mut self, point: &impl PointTrait<T = f64>) {
        if let Some(coord) = point.coord() {
            self.add_coord(&coord);
        }
    }

    pub fn add_line_string(&mut self, line_string: &impl LineStringTrait<T = f64>) {
        for coord in line_string.coords() {
            self.add_coord(&coord);
        }
    }

    pub fn add_polygon(&mut self, polygon: &impl PolygonTrait<T = f64>) {
        if let Some(exterior_ring) = polygon.exterior() {
            self.add_line_string(&exterior_ring);
        }

        for exterior in polygon.interiors() {
            self.add_line_string(&exterior)
        }
    }

    pub fn add_multi_point(&mut self, multi_point: &impl MultiPointTrait<T = f64>) {
        for point in multi_point.points() {
            self.add_point(&point);
        }
    }

    pub fn add_multi_line_string(
        &mut self,
        multi_line_string: &impl MultiLineStringTrait<T = f64>,
    ) {
        for linestring in multi_line_string.line_strings() {
            self.add_line_string(&linestring);
        }
    }

    pub fn add_multi_polygon(&mut self, multi_polygon: &impl MultiPolygonTrait<T = f64>) {
        for polygon in multi_polygon.polygons() {
            self.add_polygon(&polygon);
        }
    }

    pub fn add_triangle(&mut self, triangle: &impl TriangleTrait<T = f64>) {
        for coord in triangle.coords() {
            self.add_coord(&coord);
        }
    }

    pub fn add_line(&mut self, line: &impl LineTrait<T = f64>) {
        for coord in line.coords() {
            self.add_coord(&coord);
        }
    }

    pub fn add_geometry(&mut self, geometry: &impl GeometryTrait<T = f64>) {
        use GeometryType::*;

        match geometry.as_type() {
            Point(g) => self.add_point(g),
            LineString(g) => self.add_line_string(g),
            Polygon(g) => self.add_polygon(g),
            MultiPoint(g) => self.add_multi_point(g),
            MultiLineString(g) => self.add_multi_line_string(g),
            MultiPolygon(g) => self.add_multi_polygon(g),
            GeometryCollection(g) => self.add_geometry_collection(g),
            Rect(g) => self.add_rect(g),
            Triangle(g) => self.add_triangle(g),
            Line(g) => self.add_line(g),
        }
    }

    pub fn add_geometry_collection(
        &mut self,
        geometry_collection: &impl GeometryCollectionTrait<T = f64>,
    ) {
        for geometry in geometry_collection.geometries() {
            self.add_geometry(&geometry);
        }
    }

    pub fn add_rect(&mut self, rect: &impl RectTrait<T = f64>) {
        self.add_coord(&rect.min());
        self.add_coord(&rect.max());
    }
}

impl Default for BoundingRect {
    fn default() -> Self {
        Self::new()
    }
}

impl Add for BoundingRect {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        BoundingRect {
            minx: self.minx.min(rhs.minx),
            miny: self.miny.min(rhs.miny),
            maxx: self.maxx.max(rhs.maxx),
            maxy: self.maxy.max(rhs.maxy),
        }
    }
}

impl RectTrait for BoundingRect {
    type CoordType<'a> = geo_types::Coord;

    fn min(&self) -> Self::CoordType<'_> {
        geo_types::Coord {
            x: self.minx,
            y: self.miny,
        }
    }

    fn max(&self) -> Self::CoordType<'_> {
        geo_types::Coord {
            x: self.maxx,
            y: self.maxy,
        }
    }
}

impl GeometryTrait for BoundingRect {
    type T = f64;
    type PointType<'a>
        = UnimplementedPoint<f64>
    where
        Self: 'a;
    type LineStringType<'a>
        = UnimplementedLineString<f64>
    where
        Self: 'a;
    type PolygonType<'a>
        = UnimplementedPolygon<f64>
    where
        Self: 'a;
    type MultiPointType<'a>
        = UnimplementedMultiPoint<f64>
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = UnimplementedMultiLineString<f64>
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = UnimplementedMultiPolygon<f64>
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = UnimplementedGeometryCollection<f64>
    where
        Self: 'a;
    type RectType<'a>
        = Self
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<f64>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<f64>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn as_type(
        &self,
    ) -> GeometryType<
        '_,
        Self::PointType<'_>,
        Self::LineStringType<'_>,
        Self::PolygonType<'_>,
        Self::MultiPointType<'_>,
        Self::MultiLineStringType<'_>,
        Self::MultiPolygonType<'_>,
        Self::GeometryCollectionType<'_>,
        Self::RectType<'_>,
        Self::TriangleType<'_>,
        Self::LineType<'_>,
    > {
        GeometryType::Rect(self)
    }
}

/// Create a new RectArray using the bounding box of each geometry.
///
/// Note that this **does not** currently correctly handle the antimeridian
pub(super) fn bounding_rect(arr: &dyn GeoArrowArray) -> GeoArrowResult<Option<BoundingRect>> {
    assert_eq!(arr.len(), 1, "Assumed to be operating on a scalar");
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_array_accessor(arr.as_point()),
        LineString(_) => impl_array_accessor(arr.as_line_string()),
        Polygon(_) => impl_array_accessor(arr.as_polygon()),
        MultiPoint(_) => impl_array_accessor(arr.as_multi_point()),
        MultiLineString(_) => impl_array_accessor(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_array_accessor(arr.as_multi_polygon()),
        Geometry(_) => impl_array_accessor(arr.as_geometry()),
        GeometryCollection(_) => impl_array_accessor(arr.as_geometry_collection()),
        Rect(_) => {
            if let Some(rect) = arr.as_rect().get(0)? {
                let lower = rect.min();
                let upper = rect.max();
                let bounds = BoundingRect {
                    minx: lower.x(),
                    miny: lower.y(),
                    maxx: upper.x(),
                    maxy: upper.y(),
                };
                Ok(Some(bounds))
            } else {
                Ok(None)
            }
        }
        Wkb(_) => impl_array_accessor(arr.as_wkb::<i32>()),
        LargeWkb(_) => impl_array_accessor(arr.as_wkb::<i64>()),
        WkbView(_) => impl_array_accessor(arr.as_wkb_view()),
        Wkt(_) => impl_array_accessor(arr.as_wkt::<i32>()),
        LargeWkt(_) => impl_array_accessor(arr.as_wkt::<i64>()),
        WktView(_) => impl_array_accessor(arr.as_wkt_view()),
    }
}

/// The actual implementation of computing the bounding rect
fn impl_array_accessor<'a>(
    arr: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Option<BoundingRect>> {
    assert_eq!(arr.len(), 1, "Assumed to be operating on a scalar");
    match arr.data_type() {
        GeoArrowType::Rect(_) => unreachable!(),
        _ => {
            if let Some(item) = arr.get(0)? {
                let mut rect = BoundingRect::new();
                rect.add_geometry(&item);
                Ok(Some(rect))
            } else {
                Ok(None)
            }
        }
    }
}
