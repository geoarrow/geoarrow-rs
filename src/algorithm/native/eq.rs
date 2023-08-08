use crate::geo_traits::{
    CoordTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait,
    PointTrait, PolygonTrait,
};
use geo::CoordNum;

pub fn coord_eq<T: CoordNum>(left: impl CoordTrait<T = T>, right: impl CoordTrait<T = T>) -> bool {
    left.x_y() == right.x_y()
}

pub fn point_eq<T: CoordNum>(left: impl PointTrait<T = T>, right: impl PointTrait<T = T>) -> bool {
    left.x_y() == right.x_y()
}

pub fn line_string_eq<'a, T: CoordNum>(
    left: impl LineStringTrait<'a, T = T>,
    right: impl LineStringTrait<'a, T = T>,
) -> bool {
    if left.num_coords() != right.num_coords() {
        return false;
    }

    for coord_idx in 0..left.num_coords() {
        let left_coord = left.coord(coord_idx).unwrap();
        let right_coord = right.coord(coord_idx).unwrap();
        if !coord_eq(left_coord, right_coord) {
            return false;
        }
    }

    true
}

pub fn polygon_eq<'a, T: CoordNum>(
    left: impl PolygonTrait<'a, T = T>,
    right: impl PolygonTrait<'a, T = T>,
) -> bool {
    if left.num_interiors() != right.num_interiors() {
        return false;
    }

    if !line_string_eq(left.exterior(), right.exterior()) {
        return false;
    }

    for i in 0..left.num_interiors() {
        if !line_string_eq(left.interior(i).unwrap(), right.interior(i).unwrap()) {
            return false;
        }
    }

    true
}

pub fn multi_point_eq<'a, T: CoordNum>(
    left: impl MultiPointTrait<'a, T = T>,
    right: impl MultiPointTrait<'a, T = T>,
) -> bool {
    if left.num_points() != right.num_points() {
        return false;
    }

    for point_idx in 0..left.num_points() {
        let left_point = left.point(point_idx).unwrap();
        let right_point = right.point(point_idx).unwrap();
        if !point_eq(left_point, right_point) {
            return false;
        }
    }

    true
}

pub fn multi_line_string_eq<'a, T: CoordNum>(
    left: impl MultiLineStringTrait<'a, T = T>,
    right: impl MultiLineStringTrait<'a, T = T>,
) -> bool {
    if left.num_lines() != right.num_lines() {
        return false;
    }

    for line_idx in 0..left.num_lines() {
        if !line_string_eq(left.line(line_idx).unwrap(), right.line(line_idx).unwrap()) {
            return false;
        }
    }

    true
}

pub fn multi_polygon_eq<'a, T: CoordNum>(
    left: impl MultiPolygonTrait<'a, T = T>,
    right: impl MultiPolygonTrait<'a, T = T>,
) -> bool {
    if left.num_polygons() != right.num_polygons() {
        return false;
    }

    for polygon_idx in 0..left.num_polygons() {
        if !polygon_eq(
            left.polygon(polygon_idx).unwrap(),
            right.polygon(polygon_idx).unwrap(),
        ) {
            return false;
        }
    }

    true
}
