use std::ops::Add;

use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};
use geo_types::Coord;
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::error::Result;
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};
use geoarrow_schema::{BoxType, Dimension};

#[derive(Debug, Clone, Copy)]
pub struct BoundingRect {
    minx: f64,
    miny: f64,
    minz: f64,
    maxx: f64,
    maxy: f64,
    maxz: f64,
}

impl BoundingRect {
    /// New
    pub fn new() -> Self {
        BoundingRect {
            minx: f64::INFINITY,
            miny: f64::INFINITY,
            minz: f64::INFINITY,
            maxx: -f64::INFINITY,
            maxy: -f64::INFINITY,
            maxz: -f64::INFINITY,
        }
    }

    pub fn minx(&self) -> f64 {
        self.minx
    }

    pub fn miny(&self) -> f64 {
        self.miny
    }

    pub fn minz(&self) -> Option<f64> {
        if self.minz == f64::INFINITY {
            None
        } else {
            Some(self.minz)
        }
    }

    pub fn maxx(&self) -> f64 {
        self.maxx
    }

    pub fn maxy(&self) -> f64 {
        self.maxy
    }

    pub fn maxz(&self) -> Option<f64> {
        if self.maxz == -f64::INFINITY {
            None
        } else {
            Some(self.maxz)
        }
    }

    pub fn add_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        let x = coord.x();
        let y = coord.y();
        let z = coord.nth(2);

        if x < self.minx {
            self.minx = x;
        }
        if y < self.miny {
            self.miny = y;
        }
        if let Some(z) = z {
            if z < self.minz {
                self.minz = z;
            }
        }

        if x > self.maxx {
            self.maxx = x;
        }
        if y > self.maxy {
            self.maxy = y;
        }
        if let Some(z) = z {
            if z > self.maxz {
                self.maxz = z;
            }
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
            Triangle(_) | Line(_) => todo!(),
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

    pub fn update(&mut self, other: &BoundingRect) {
        self.add_rect(other)
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
            minz: self.minz.min(rhs.minz),
            maxx: self.maxx.max(rhs.maxx),
            maxy: self.maxy.max(rhs.maxy),
            maxz: self.maxz.max(rhs.maxz),
        }
    }
}

impl RectTrait for BoundingRect {
    type T = f64;
    type CoordType<'a> = Coord;

    fn dim(&self) -> geo_traits::Dimensions {
        if self.minz().is_some() && self.maxz().is_some() {
            geo_traits::Dimensions::Xyz
        } else {
            geo_traits::Dimensions::Xy
        }
    }

    fn min(&self) -> Self::CoordType<'_> {
        Coord {
            x: self.minx,
            y: self.miny,
        }
    }

    fn max(&self) -> Self::CoordType<'_> {
        Coord {
            x: self.maxx,
            y: self.maxy,
        }
    }
}

/// Create a new RectArray using the bounding box of each geometry.
///
/// Note that this **does not** currently correctly handle the antimeridian
pub(crate) fn bounding_rect(arr: &dyn GeoArrowArray) -> Result<RectArray> {
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
        Rect(_) => Ok(arr.as_rect().clone()),
        Wkb(_) => impl_array_accessor(arr.as_wkb::<i32>()),
        LargeWkb(_) => impl_array_accessor(arr.as_wkb::<i64>()),
        Wkt(_) => impl_array_accessor(arr.as_wkt::<i32>()),
        LargeWkt(_) => impl_array_accessor(arr.as_wkt::<i64>()),
    }
}

/// The actual implementation of computing the bounding rect
fn impl_array_accessor<'a>(arr: &'a impl ArrayAccessor<'a>) -> Result<RectArray> {
    match arr.data_type() {
        GeoArrowType::Rect(_) => unreachable!(),
        _ => {
            let mut builder = RectBuilder::with_capacity(
                BoxType::new(Dimension::XY, Default::default()),
                arr.len(),
            );
            for item in arr.iter() {
                if let Some(item) = item {
                    let mut rect = BoundingRect::new();
                    rect.add_geometry(&item?);
                    builder.push_rect(Some(&rect));
                } else {
                    builder.push_null();
                }
            }
            Ok(builder.finish())
        }
    }
}

/// Get the total bounds (i.e. minx, miny, maxx, maxy) of the entire geoarrow array.
pub(crate) fn total_bounds(arr: &dyn GeoArrowArray) -> Result<BoundingRect> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_total_bounds(arr.as_point()),
        LineString(_) => impl_total_bounds(arr.as_line_string()),
        Polygon(_) => impl_total_bounds(arr.as_polygon()),
        MultiPoint(_) => impl_total_bounds(arr.as_multi_point()),
        MultiLineString(_) => impl_total_bounds(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_total_bounds(arr.as_multi_polygon()),
        Geometry(_) => impl_total_bounds(arr.as_geometry()),
        GeometryCollection(_) => impl_total_bounds(arr.as_geometry_collection()),
        Rect(_) => impl_total_bounds(arr.as_rect()),
        Wkb(_) => impl_total_bounds(arr.as_wkb::<i32>()),
        LargeWkb(_) => impl_total_bounds(arr.as_wkb::<i64>()),
        Wkt(_) => impl_total_bounds(arr.as_wkt::<i32>()),
        LargeWkt(_) => impl_total_bounds(arr.as_wkt::<i64>()),
    }
}

/// The actual implementation of computing the total bounds
fn impl_total_bounds<'a>(arr: &'a impl ArrayAccessor<'a>) -> Result<BoundingRect> {
    let mut rect = BoundingRect::new();

    for item in arr.iter().flatten() {
        rect.add_geometry(&item?);
    }

    Ok(rect)
}
