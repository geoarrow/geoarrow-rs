use crate::geo_traits::{
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait,
};
use crate::scalar::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use arrow_array::OffsetSizeTrait;
use geo::{coord, Rect};

#[derive(Debug, Clone, Copy)]
struct BoundingRect {
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

    pub fn update(&mut self, point: impl PointTrait<T = f64>) {
        if point.x() < self.minx {
            self.minx = point.x();
        }
        if point.y() < self.miny {
            self.miny = point.y();
        }
        if point.x() > self.maxx {
            self.maxx = point.x();
        }
        if point.y() > self.maxy {
            self.maxy = point.y();
        }
    }
}

impl Default for BoundingRect {
    fn default() -> Self {
        Self::new()
    }
}

impl From<BoundingRect> for Rect {
    fn from(value: BoundingRect) -> Self {
        let min_coord = coord! { x: value.minx, y: value.miny };
        let max_coord = coord! { x: value.maxx, y: value.maxy };
        Rect::new(min_coord, max_coord)
    }
}

impl From<BoundingRect> for ([f64; 2], [f64; 2]) {
    fn from(value: BoundingRect) -> Self {
        ([value.minx, value.miny], [value.maxx, value.maxy])
    }
}

pub fn bounding_rect_point(geom: &'_ Point) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    rect.update(geom);
    rect.into()
}

pub fn bounding_rect_multipoint<O: OffsetSizeTrait>(
    geom: &'_ MultiPoint<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for point in geom.points() {
        rect.update(point);
    }
    rect.into()
}

pub fn bounding_rect_linestring<O: OffsetSizeTrait>(
    geom: &'_ LineString<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for point in geom.coords() {
        rect.update(point);
    }
    rect.into()
}

pub fn bounding_rect_multilinestring<O: OffsetSizeTrait>(
    geom: &'_ MultiLineString<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for linestring in geom.lines() {
        for point in linestring.coords() {
            rect.update(point);
        }
    }
    rect.into()
}

pub fn bounding_rect_polygon<O: OffsetSizeTrait>(geom: &'_ Polygon<O>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    let exterior_ring = geom.exterior().unwrap();
    for point in exterior_ring.coords() {
        rect.update(point);
    }

    for linestring in geom.interiors() {
        for point in linestring.coords() {
            rect.update(point);
        }
    }
    rect.into()
}

pub fn bounding_rect_multipolygon<O: OffsetSizeTrait>(
    geom: &'_ MultiPolygon<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for polygon in geom.polygons() {
        let exterior_ring = polygon.exterior().unwrap();
        for point in exterior_ring.coords() {
            rect.update(point);
        }

        for interior in polygon.interiors() {
            for point in interior.coords() {
                rect.update(point);
            }
        }
    }

    rect.into()
}

// TODO: add tests from geo
