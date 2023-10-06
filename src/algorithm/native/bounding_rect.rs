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
    for geom_idx in 0..geom.num_points() {
        let point = geom.point(geom_idx).unwrap();
        rect.update(point);
    }
    rect.into()
}

pub fn bounding_rect_linestring<O: OffsetSizeTrait>(
    geom: &'_ LineString<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_coords() {
        let point = geom.coord(geom_idx).unwrap();
        rect.update(point);
    }
    rect.into()
}

pub fn bounding_rect_multilinestring<O: OffsetSizeTrait>(
    geom: &'_ MultiLineString<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_lines() {
        let linestring = geom.line(geom_idx).unwrap();
        for coord_idx in 0..linestring.num_coords() {
            let point = linestring.coord(coord_idx).unwrap();
            rect.update(point);
        }
    }
    rect.into()
}

pub fn bounding_rect_polygon<O: OffsetSizeTrait>(geom: &'_ Polygon<O>) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    let exterior_ring = geom.exterior().unwrap();
    for coord_idx in 0..exterior_ring.num_coords() {
        let point = exterior_ring.coord(coord_idx).unwrap();
        rect.update(point);
    }

    for interior_idx in 0..geom.num_interiors() {
        let linestring = geom.interior(interior_idx).unwrap();
        for coord_idx in 0..linestring.num_coords() {
            let point = linestring.coord(coord_idx).unwrap();
            rect.update(point);
        }
    }
    rect.into()
}

pub fn bounding_rect_multipolygon<O: OffsetSizeTrait>(
    geom: &'_ MultiPolygon<O>,
) -> ([f64; 2], [f64; 2]) {
    let mut rect = BoundingRect::new();
    for geom_idx in 0..geom.num_polygons() {
        let polygon = geom.polygon(geom_idx).unwrap();

        let exterior_ring = polygon.exterior().unwrap();
        for coord_idx in 0..exterior_ring.num_coords() {
            let point = exterior_ring.coord(coord_idx).unwrap();
            rect.update(point);
        }

        for interior_idx in 0..polygon.num_interiors() {
            let linestring = polygon.interior(interior_idx).unwrap();
            for coord_idx in 0..linestring.num_coords() {
                let point = linestring.coord(coord_idx).unwrap();
                rect.update(point);
            }
        }
    }

    rect.into()
}

// TODO: add tests from geo
