use std::ops::Add;

use geo::{Coord, Rect};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};

use crate::array::*;
use crate::datatypes::Dimension;
use crate::trait_::ArrayAccessor;

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

impl From<BoundingRect> for Rect {
    fn from(value: BoundingRect) -> Self {
        let min_coord = Coord {
            x: value.minx,
            y: value.miny,
        };
        let max_coord = Coord {
            x: value.maxx,
            y: value.maxy,
        };
        Rect::new(min_coord, max_coord)
    }
}

impl From<BoundingRect> for ([f64; 2], [f64; 2]) {
    fn from(value: BoundingRect) -> Self {
        ([value.minx, value.miny], [value.maxx, value.maxy])
    }
}

impl From<BoundingRect> for (f64, f64, f64, f64) {
    fn from(value: BoundingRect) -> Self {
        (value.minx, value.miny, value.maxx, value.maxy)
    }
}

pub fn bounding_rect_point(geom: &impl PointTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_point(geom);
    rect.into()
}

pub fn bounding_rect_multipoint(geom: &impl MultiPointTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_multi_point(geom);
    rect.into()
}

pub fn bounding_rect_linestring(geom: &impl LineStringTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_line_string(geom);
    rect.into()
}

pub fn bounding_rect_multilinestring(
    geom: &impl MultiLineStringTrait<T = f64>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_multi_line_string(geom);
    rect.into()
}

pub fn bounding_rect_polygon(geom: &impl PolygonTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_polygon(geom);
    rect.into()
}

pub fn bounding_rect_multipolygon(geom: &impl MultiPolygonTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_multi_polygon(geom);
    rect.into()
}

pub fn bounding_rect_geometry(geom: &impl GeometryTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_geometry(geom);
    rect.into()
}

pub fn bounding_rect_geometry_collection(
    geom: &impl GeometryCollectionTrait<T = f64>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_geometry_collection(geom);
    rect.into()
}

pub fn bounding_rect_rect(geom: &impl RectTrait<T = f64>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.add_rect(geom);
    rect.into()
}

/// Calculation of the bounding rectangle of a geometry.
pub trait BoundingRectArray {
    type Output;

    fn bounding_rect(&self) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! array_impl {
    ($type:ty, $bounding_rect_fn:ident) => {
        impl BoundingRectArray for $type {
            type Output = RectArray;

            fn bounding_rect(&self) -> Self::Output {
                let mut builder = RectBuilder::with_capacity_and_options(
                    Dimension::XY,
                    self.len(),
                    self.metadata().clone(),
                );
                for geom in self.iter() {
                    if let Some(geom) = geom {
                        let ([minx, miny], [maxx, maxy]) = $bounding_rect_fn(&geom);
                        builder.push_box2d(Some([minx, miny, maxx, maxy]));
                    } else {
                        builder.push_null();
                    }
                }

                builder.finish()
            }
        }
    };
}

array_impl!(PointArray, bounding_rect_point);
array_impl!(LineStringArray, bounding_rect_linestring);
array_impl!(PolygonArray, bounding_rect_polygon);
array_impl!(MultiPointArray, bounding_rect_multipoint);
array_impl!(MultiLineStringArray, bounding_rect_multilinestring);
array_impl!(MultiPolygonArray, bounding_rect_multipolygon);
array_impl!(MixedGeometryArray, bounding_rect_geometry);
array_impl!(GeometryCollectionArray, bounding_rect_geometry_collection);
array_impl!(GeometryArray, bounding_rect_geometry);

impl BoundingRectArray for RectArray {
    type Output = RectArray;

    fn bounding_rect(&self) -> Self::Output {
        self.clone()
    }
}

// TODO: add tests from geo
