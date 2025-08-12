use arrow_array::Float64Array;
use geo::{Euclidean, Geometry, Length};
use geo_traits::to_geo::{ToGeoLine, ToGeoLineString, ToGeoMultiLineString};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::to_geo::geometry_to_geo;

pub fn length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _length_impl)
}

pub fn _length_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Float64Array> {
    let mut result = Float64Array::builder(array.len());
    for geom in array.iter() {
        if let Some(geom) = geom {
            let geom = geometry_to_geo(&geom?)?;
            match geom {
                Geometry::Line(l) => {
                    result.append_value(Euclidean.length(&l.to_line()));
                }
                Geometry::LineString(ls) => {
                    result.append_value(Euclidean.length(&ls.to_line_string()));
                }
                Geometry::MultiLineString(mls) => {
                    result.append_value(Euclidean.length(&mls.to_multi_line_string()));
                }
                _ => {
                    result.append_value(0.0);
                }
            }
        } else {
            result.append_null();
        }
    }
    Ok(result.finish())
}

#[cfg(test)]
mod test {

    use geo::{Euclidean, Length};
    use geoarrow_array::{
        array::PointArray,
        builder::{LineStringBuilder, MultiLineStringBuilder, PointBuilder, WkbBuilder},
    };
    use geoarrow_schema::{CoordType, WkbType};
    use geoarrow_schema::{Dimension, PointType};

    use super::*;

    #[test]
    fn test_point() {
        let point_type = PointType::new(Dimension::XY, Default::default());
        let mut builder = PointBuilder::new(point_type);

        builder.push_point(Some(&geo_types::point!(x: 0., y: 1.)));
        builder.push_point(Some(&geo_types::point!(x: 2., y: 3.)));
        builder.push_point(Some(&geo_types::point!(x: 4., y: 5.)));

        let point_array: PointArray = builder.finish();
        let result = length(&point_array).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 0.0);
        assert_eq!(result.value(1), 0.0);
        assert_eq!(result.value(2), 0.0);
    }

    #[test]
    fn test_linestring() {
        let mut linestring_builder = LineStringBuilder::new(
            geoarrow_schema::LineStringType::new(Dimension::XY, Default::default())
                .with_coord_type(CoordType::Separated),
        );
        let linestring_1 = geo_types::LineString(vec![
            geo_types::Coord { x: 0.0, y: 0.0 },
            geo_types::Coord { x: 3.0, y: 9.0 },
        ]);
        linestring_builder.push_geometry(Some(&linestring_1));
        let linestring_2 = geo_types::LineString(vec![
            geo_types::Coord { x: 0.0, y: 0.0 },
            geo_types::Coord { x: 4.0, y: 5.0 },
        ]);
        linestring_builder.push_geometry(Some(&linestring_2));
        let linestring_array = linestring_builder.finish();

        let result = length(&linestring_array).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), Euclidean.length(&linestring_1));
        assert_eq!(result.value(1), Euclidean.length(&linestring_2));
    }

    #[test]
    fn test_multilinestring() {
        let mut multi_linestring_builder = MultiLineStringBuilder::new(
            geoarrow_schema::MultiLineStringType::new(Dimension::XY, Default::default())
                .with_coord_type(CoordType::Separated),
        );
        let linestring_1 = geo_types::LineString(vec![
            geo_types::Coord { x: 0.0, y: 9.0 },
            geo_types::Coord { x: 3.0, y: 4.0 },
        ]);
        let linestring_2 = geo_types::LineString(vec![
            geo_types::Coord { x: 0.0, y: 0.0 },
            geo_types::Coord { x: 4.0, y: 3.0 },
        ]);
        let multi_linestring_1 =
            geo_types::MultiLineString(vec![linestring_1.clone(), linestring_2.clone()]);
        let linestring_3 = geo_types::LineString(vec![
            geo_types::Coord { x: 1.0, y: 5.0 },
            geo_types::Coord { x: 5.0, y: 6.0 },
        ]);
        let multi_linestring_2 = geo_types::MultiLineString(vec![linestring_3.clone()]);

        let _ = multi_linestring_builder.push_geometry(Some(&multi_linestring_1));
        let _ = multi_linestring_builder.push_geometry(Some(&multi_linestring_2));

        let multi_linestring_array = multi_linestring_builder.finish();
        let result = length(&multi_linestring_array).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result.value(0),
            Euclidean.length(&linestring_1) + Euclidean.length(&linestring_2)
        );
        assert_eq!(result.value(1), Euclidean.length(&linestring_3));
    }

    #[test]
    fn test_wkb_linestring() {
        let mut wkb_builder: WkbBuilder<i32> =
            geoarrow_array::builder::WkbBuilder::new(WkbType::new(Default::default()));
        let linestring_1 = geo_types::LineString(vec![
            geo_types::Coord { x: 0.0, y: 0.0 },
            geo_types::Coord { x: 3.0, y: 4.0 },
        ]);
        let linestring_2 = geo_types::LineString(vec![
            geo_types::Coord { x: 0.0, y: 0.0 },
            geo_types::Coord { x: 4.0, y: 5.0 },
        ]);
        let _ = wkb_builder.push_geometry(Some(&linestring_1));
        let _ = wkb_builder.push_geometry(Some(&linestring_2));
        let wkb_array = wkb_builder.finish();

        let result = length(&wkb_array).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(result.value(0), Euclidean.length(&linestring_1));
        assert_eq!(result.value(1), Euclidean.length(&linestring_2));
    }

    #[test]
    fn test_wkb_point() {
        let mut wkb_builder: WkbBuilder<i32> =
            geoarrow_array::builder::WkbBuilder::new(WkbType::new(Default::default()));
        let point_1 = geo_types::Point::new(1.0, 2.0);
        let point_2 = geo_types::Point::new(3.0, 4.0);
        let _ = wkb_builder.push_geometry(Some(&point_1));
        let _ = wkb_builder.push_geometry(Some(&point_2));
        let wkb_array = wkb_builder.finish();

        let result = length(&wkb_array).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(result.value(0), 0.0);
        assert_eq!(result.value(1), 0.0);
    }
}
