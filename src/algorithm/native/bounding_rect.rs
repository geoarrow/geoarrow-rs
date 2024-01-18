use crate::geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};
use geo::{Coord, Rect};

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

    pub fn add_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        if coord.x() < self.minx {
            self.minx = coord.x();
        }
        if coord.y() < self.miny {
            self.miny = coord.y();
        }
        if coord.x() > self.maxx {
            self.maxx = coord.x();
        }
        if coord.y() > self.maxy {
            self.maxy = coord.y();
        }
    }

    pub fn add_point(&mut self, point: &impl PointTrait<T = f64>) {
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
        for linestring in multi_line_string.lines() {
            self.add_line_string(&linestring);
        }
    }

    pub fn add_multi_polygon(&mut self, multi_polygon: &impl MultiPolygonTrait<T = f64>) {
        for polygon in multi_polygon.polygons() {
            self.add_polygon(&polygon);
        }
    }

    pub fn add_geometry(&mut self, geometry: &impl GeometryTrait<T = f64>) {
        match geometry.as_type() {
            GeometryType::Point(g) => self.add_point(g),
            GeometryType::LineString(g) => self.add_line_string(g),
            GeometryType::Polygon(g) => self.add_polygon(g),
            GeometryType::MultiPoint(g) => self.add_multi_point(g),
            GeometryType::MultiLineString(g) => self.add_multi_line_string(g),
            GeometryType::MultiPolygon(g) => self.add_multi_polygon(g),
            GeometryType::GeometryCollection(g) => self.add_geometry_collection(g),
            GeometryType::Rect(g) => self.add_rect(g),
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
        self.add_coord(&rect.lower());
        self.add_coord(&rect.upper());
    }
}

impl Default for BoundingRect {
    fn default() -> Self {
        Self::new()
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

// TODO: add tests from geo
