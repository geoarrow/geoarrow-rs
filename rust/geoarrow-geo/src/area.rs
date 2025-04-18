use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_buffer::NullBuffer;
use geo::{Area, Orient};
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
    use geo::{Polygon, polygon};
    use geoarrow_array::array::PolygonArray;
    use geoarrow_array::builder::PolygonBuilder;
    use geoarrow_schema::{CoordType, Dimension, PolygonType};

    use super::*;

    fn p0() -> Polygon {
        polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ]
    }

    fn p1() -> Polygon {
        polygon!(
            exterior: [
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            interiors: [
                [
                    (x: -110., y: 44.),
                    (x: -110., y: 42.),
                    (x: -105., y: 42.),
                    (x: -105., y: 44.),
                ],
            ],
        )
    }

    fn p_array() -> PolygonArray {
        let geoms = vec![p0(), p1()];
        let typ = PolygonType::new(CoordType::Interleaved, Dimension::XY, Default::default());
        PolygonBuilder::from_polygons(&geoms, typ).finish()
    }

    #[test]
    fn test_signed_area() {
        let g = geo::wkt! {  POLYGON ((35. 10., 45. 45., 15. 40., 10. 20., 35. 10.), (20. 30., 35. 35., 30. 20., 20. 30.)) };
        let out = g.signed_area();
        dbg!(out);
    }

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
        let p = geo_arr.value(1).unwrap();
        // p.num
        // let signed = signed_area(&geo_arr).unwrap();
        // let unsigned = unsigned_area(&geo_arr).unwrap();

        // dbg!(&signed);
        // dbg!(&unsigned);
    }

    #[test]
    fn area_specialized() {
        let arr = p_array();
        let area = unsigned_area(&arr).unwrap();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
