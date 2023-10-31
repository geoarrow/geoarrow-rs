use crate::geo_traits::{
    CoordTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait,
    PointTrait, PolygonTrait,
};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use geo::CoordFloat;

#[inline]
pub fn coord_eq_allow_nan<T: CoordFloat>(
    left: impl CoordTrait<T = T>,
    right: impl CoordTrait<T = T>,
) -> bool {
    // Specifically check for NaN because two points defined to be
    // TODO: in the future add an `is_empty` to the PointTrait and then you shouldn't check for
    // NaN manually
    if left.x().is_nan() && right.x().is_nan() && left.y().is_nan() && right.y().is_nan() {
        return true;
    }

    left.x_y() == right.x_y()
}

#[inline]
pub fn coord_eq<T: CoordFloat>(
    left: impl CoordTrait<T = T>,
    right: impl CoordTrait<T = T>,
) -> bool {
    left.x_y() == right.x_y()
}

#[inline]
pub fn point_eq<T: CoordFloat>(
    left: impl PointTrait<T = T>,
    right: impl PointTrait<T = T>,
    allow_nan_equal: bool,
) -> bool {
    if allow_nan_equal {
        // Specifically check for NaN because two points defined to be
        // TODO: in the future add an `is_empty` to the PointTrait and then you shouldn't check for
        // NaN manually
        if left.x().is_nan() && right.x().is_nan() && left.y().is_nan() && right.y().is_nan() {
            return true;
        }
    }

    left.x_y() == right.x_y()
}

#[inline]
pub fn line_string_eq<'a, T: CoordFloat>(
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

#[inline]
pub fn polygon_eq<'a, T: CoordFloat>(
    left: impl PolygonTrait<'a, T = T>,
    right: impl PolygonTrait<'a, T = T>,
) -> bool {
    if left.num_interiors() != right.num_interiors() {
        return false;
    }

    match (left.exterior(), right.exterior()) {
        (None, None) => (),
        (Some(_), None) => {
            return false;
        }
        (None, Some(_)) => {
            return false;
        }
        (Some(left), Some(right)) => {
            if !line_string_eq(left, right) {
                return false;
            }
        }
    };

    for i in 0..left.num_interiors() {
        if !line_string_eq(left.interior(i).unwrap(), right.interior(i).unwrap()) {
            return false;
        }
    }

    true
}

#[inline]
pub fn multi_point_eq<'a, T: CoordFloat>(
    left: impl MultiPointTrait<'a, T = T>,
    right: impl MultiPointTrait<'a, T = T>,
) -> bool {
    if left.num_points() != right.num_points() {
        return false;
    }

    for point_idx in 0..left.num_points() {
        let left_point = left.point(point_idx).unwrap();
        let right_point = right.point(point_idx).unwrap();
        if !point_eq(left_point, right_point, false) {
            return false;
        }
    }

    true
}

#[inline]
pub fn multi_line_string_eq<'a, T: CoordFloat>(
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

#[inline]
pub fn multi_polygon_eq<'a, T: CoordFloat>(
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

pub(crate) fn offset_buffer_eq<O: OffsetSizeTrait>(
    left: &OffsetBuffer<O>,
    right: &OffsetBuffer<O>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }

    for (o1, o2) in left.iter().zip(right.iter()) {
        if o1 != o2 {
            return false;
        }
    }

    true
}
