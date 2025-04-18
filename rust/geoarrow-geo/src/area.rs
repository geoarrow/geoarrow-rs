use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_buffer::NullBuffer;
use geo::Area;
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::error::Result;
use geoarrow_array::{ArrayAccessor, GeoArrowType};

pub fn unsigned_area<'a>(array: &'a impl ArrayAccessor<'a>) -> Result<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(_zeros(array.len(), array.nulls().cloned()))
        }
        _ => _unsigned_area_impl(array, Area::unsigned_area),
    }
}

pub fn signed_area<'a>(array: &'a impl ArrayAccessor<'a>) -> Result<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(_zeros(array.len(), array.nulls().cloned()))
        }
        _ => _unsigned_area_impl(array, Area::signed_area),
    }
}

fn _zeros(len: usize, nulls: Option<NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls)
}

fn _unsigned_area_impl<'a, F: Fn(&geo::Geometry) -> f64>(
    array: &'a impl ArrayAccessor<'a>,
    area_fn: F,
) -> Result<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_geometry();
            builder.append_value(area_fn(&geo_geom));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
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
