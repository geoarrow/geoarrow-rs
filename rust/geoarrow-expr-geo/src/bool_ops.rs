use geo::algorithm::bool_ops::{BooleanOps, OpType};
use geoarrow_array::array::MultiPolygonArray;
use geoarrow_array::builder::MultiPolygonBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};

use crate::util::check_same_len;
use crate::util::downcast::downcast_geoarrow_array_two_args;
use crate::util::to_geo::geometry_to_geo;

/// Element-wise `left[i] ∩ right[i]`.
///
/// Non-areal operands give a null row, which a caller cannot tell apart from
/// a null input.
pub fn intersection(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    boolean_op(left_array, right_array, OpType::Intersection, coord_type)
}

/// Element-wise `left[i] ∪ right[i]`.
///
/// Non-areal operands give a null row, which a caller cannot tell apart from
/// a null input.
pub fn union(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    boolean_op(left_array, right_array, OpType::Union, coord_type)
}

/// Element-wise `left[i] - right[i]`. The operands do not commute.
///
/// Non-areal operands give a null row, which a caller cannot tell apart from
/// a null input.
pub fn difference(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    boolean_op(left_array, right_array, OpType::Difference, coord_type)
}

/// Element-wise symmetric difference: the regions in exactly one of `left[i]`
/// and `right[i]`.
///
/// Non-areal operands give a null row, which a caller cannot tell apart from
/// a null input.
pub fn xor(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    boolean_op(left_array, right_array, OpType::Xor, coord_type)
}

fn boolean_op(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
    op: OpType,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    check_same_len(left_array, right_array)?;
    downcast_geoarrow_array_two_args!(left_array, right_array, boolean_op_impl, op, coord_type)
}

fn boolean_op_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
    op: OpType,
    coord_type: CoordType,
) -> GeoArrowResult<MultiPolygonArray> {
    let typ = MultiPolygonType::new(Dimension::XY, left_array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = MultiPolygonBuilder::new(typ);

    for (left, right) in left_array.iter().zip(right_array.iter()) {
        let out = match (left, right) {
            (Some(left), Some(right)) => {
                let left = geometry_to_geo(&left?)?;
                let right = geometry_to_geo(&right?)?;
                match (as_multi_polygon(&left), as_multi_polygon(&right)) {
                    (Some(left), Some(right)) => Some(left.boolean_op(&right, op)),
                    _ => None,
                }
            }
            _ => None,
        };
        builder.push_multi_polygon(out.as_ref())?;
    }

    Ok(builder.finish())
}

/// `geo` defines the boolean ops on areal geometries only. A caller gets a null
/// row for anything else, which it cannot tell apart from a null input.
fn as_multi_polygon(geom: &geo::Geometry) -> Option<geo::MultiPolygon> {
    match geom {
        geo::Geometry::Polygon(p) => Some(geo::MultiPolygon::new(vec![p.clone()])),
        geo::Geometry::MultiPolygon(mp) => Some(mp.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use geo::{Area, Geometry, point, polygon};

    use super::*;
    use crate::test_util::{PLAIN_SQUARE, geometry_array, xyzm_polygon_array};

    /// Square `[0,2]x[0,2]`, area 4.
    fn square_a() -> Geometry {
        Geometry::from(polygon![(x: 0., y: 0.), (x: 2., y: 0.), (x: 2., y: 2.), (x: 0., y: 2.)])
    }

    /// Square `[1,3]x[1,3]`, area 4. It overlaps `square_a` on `[1,2]x[1,2]`, area 1.
    fn square_b() -> Geometry {
        Geometry::from(polygon![(x: 1., y: 1.), (x: 3., y: 1.), (x: 3., y: 3.), (x: 1., y: 3.)])
    }

    fn total_area(result: &MultiPolygonArray) -> f64 {
        result
            .iter()
            .flatten()
            .map(|geom| geometry_to_geo(&geom.unwrap()).unwrap().unsigned_area())
            .sum()
    }

    fn assert_area(result: GeoArrowResult<MultiPolygonArray>, expected: f64) {
        assert!((total_area(&result.unwrap()) - expected).abs() < 1e-9);
    }

    /// The two squares are congruent, thus a swap of the operands would go
    /// unnoticed. A third, containing square pins the direction of `difference`.
    #[test]
    fn areas_of_the_four_ops() {
        let a = geometry_array(vec![Some(square_a())]);
        let b = geometry_array(vec![Some(square_b())]);
        let big = geometry_array(vec![Some(Geometry::from(
            polygon![(x: 0., y: 0.), (x: 4., y: 0.), (x: 4., y: 4.), (x: 0., y: 4.)],
        ))]);

        assert_area(intersection(&a, &b, CoordType::Interleaved), 1.0);
        assert_area(union(&a, &b, CoordType::Interleaved), 7.0);
        assert_area(difference(&a, &b, CoordType::Interleaved), 3.0);
        assert_area(xor(&a, &b, CoordType::Interleaved), 6.0);
        assert_area(difference(&big, &a, CoordType::Interleaved), 12.0);
        assert_area(difference(&a, &big, CoordType::Interleaved), 0.0);
    }

    /// Row 3 pins the one case that is present but empty, which a caller must be
    /// able to tell apart from the null rows above it.
    #[test]
    fn null_and_non_areal_produce_null() {
        let left = geometry_array(vec![
            None,
            Some(square_a()),
            Some(Geometry::from(point!(x: 0.5, y: 0.5))),
            Some(square_a()),
        ]);
        let right = geometry_array(vec![
            Some(square_b()),
            None,
            Some(square_b()),
            Some(Geometry::from(
                polygon![(x: 100., y: 100.), (x: 102., y: 100.), (x: 102., y: 102.), (x: 100., y: 102.)],
            )),
        ]);

        let result = intersection(&left, &right, CoordType::Interleaved).unwrap();
        let rows: Vec<_> = result.iter().collect();
        assert!(rows[0].is_none());
        assert!(rows[1].is_none());
        assert!(rows[2].is_none());
        assert!(rows[3].is_some());
        assert!(total_area(&result) < 1e-9);
    }

    /// `geo` overlays in XY only, thus an XYZM input gives an XY output.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);
        let result = union(&arr, &arr, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
    }

    #[test]
    fn length_mismatch_errors() {
        let left = geometry_array(vec![Some(square_a())]);
        let right = geometry_array(vec![]);
        assert!(intersection(&left, &right, CoordType::Interleaved).is_err());
    }
}
