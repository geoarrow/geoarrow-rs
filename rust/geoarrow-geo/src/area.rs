use arrow_array::builder::Float64Builder;
use arrow_array::Float64Array;
use geo::Area;
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::error::Result;
use geoarrow_array::{ArrayAccessor, GeoArrowType};

pub fn unsigned_area<'a>(array: &'a impl ArrayAccessor<'a>) -> Result<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(geom?.to_geometry().unsigned_area());
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

pub fn unsigned_area_specialized<'a>(array: &'a impl ArrayAccessor<'a>) -> Result<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            let values = vec![0.0f64; array.len()];
            Ok(Float64Array::new(values.into(), array.nulls().cloned()))
        }
        _ => unsigned_area(array),
    }
}

#[cfg(test)]
mod test {
    use geo::{polygon, Polygon};
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
    fn area() {
        let arr = p_array();
        let area = unsigned_area(&arr).unwrap();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }

    #[test]
    fn area_specialized() {
        let arr = p_array();
        let area = unsigned_area_specialized(&arr).unwrap();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
