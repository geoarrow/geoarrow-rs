use arrow_array::Float64Array;
use geo::Area;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;

use crate::util::{map_to_f64, zeros};

pub fn unsigned_area(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _unsigned_area_impl)
}

pub fn signed_area(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _signed_area_impl)
}

fn _unsigned_area_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(zeros(array.len(), array.logical_nulls()))
        }
        _ => map_to_f64(array, Area::unsigned_area),
    }
}

fn _signed_area_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(zeros(array.len(), array.logical_nulls()))
        }
        _ => map_to_f64(array, Area::signed_area),
    }
}

#[cfg(test)]
mod test {
    use arrow_array::create_array;
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn area_zero() {
        let geo_arr = geoarrow_array::test::point::array(CoordType::Interleaved, Dimension::XY);
        let signed = signed_area(&geo_arr).unwrap();
        let unsigned = unsigned_area(&geo_arr).unwrap();

        let expected = create_array!(Float64, [Some(0.0), Some(0.0), None, Some(0.0)]);
        assert_eq!(&signed, expected.as_ref());
        assert_eq!(&unsigned, expected.as_ref());
    }

    #[test]
    fn area_polygon() {
        let geo_arr = geoarrow_array::test::polygon::array(CoordType::Separated, Dimension::XY);
        let signed = signed_area(&geo_arr).unwrap();
        let unsigned = unsigned_area(&geo_arr).unwrap();

        let expected = create_array!(Float64, [Some(550.0), Some(675.0), None, Some(0.0)]);
        assert_eq!(&signed, expected.as_ref());
        assert_eq!(&unsigned, expected.as_ref());
    }
}
