use arrow_array::Float64Array;
use geo::{Euclidean, Geodesic, Haversine, Length, Rhumb};
use geo_traits::to_geo::{ToGeoLine, ToGeoLineString, ToGeoMultiLineString};
use geo_traits::{GeometryCollectionTrait, GeometryTrait};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

/// Compute the euclidean length of linear geometries in a GeoArrowArray.
///
/// Only Line, LineString and MultiLineString geometries, and GeometryCollections
/// of them, will have non-zero lengths. Other geometry types (including polygons)
/// will return a length of 0.0.
pub fn euclidean_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, length_impl, &Euclidean)
}

/// Compute the geodesic length, in meters, of linear geometries. Coordinates are
/// interpreted as lon/lat degrees.
pub fn geodesic_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, length_impl, &Geodesic)
}

/// Compute the great-circle length, in meters, of linear geometries. Coordinates
/// are interpreted as lon/lat degrees.
pub fn haversine_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, length_impl, &Haversine)
}

/// Compute the rhumb-line length, in meters, of linear geometries. Coordinates
/// are interpreted as lon/lat degrees.
pub fn rhumb_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, length_impl, &Rhumb)
}

/// `geo`'s `Length` has no GeometryCollection impl, so a collection is summed
/// member by member here rather than falling to the catch-all zero.
fn geom_length<M: Length<f64>>(geom: &impl GeometryTrait<T = f64>, metric: &M) -> f64 {
    use geo_traits::GeometryType::*;
    match geom.as_type() {
        Line(l) => metric.length(&l.to_line()),
        LineString(ls) => metric.length(&ls.to_line_string()),
        MultiLineString(mls) => metric.length(&mls.to_multi_line_string()),
        GeometryCollection(gc) => gc.geometries().map(|g| geom_length(&g, metric)).sum(),
        _ => 0.0,
    }
}

fn length_impl<'a, M: Length<f64>>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    metric: &M,
) -> GeoArrowResult<Float64Array> {
    let mut result = Float64Array::builder(array.len());
    for geom in array.iter() {
        if let Some(geom) = geom {
            result.append_value(geom_length(&geom?, metric));
        } else {
            result.append_null();
        }
    }
    Ok(result.finish())
}

#[cfg(test)]
mod test {

    use arrow_array::Array;
    use geo::{
        Euclidean, Geodesic, Geometry, GeometryCollection, Haversine, Length, LineString,
        MultiLineString, Point, Rhumb,
    };
    use geoarrow_array::array::PointArray;
    use geoarrow_array::builder::{
        LineStringBuilder, MultiLineStringBuilder, PointBuilder, WkbBuilder,
    };
    use geoarrow_schema::{CoordType, Dimension, PointType, WkbType};

    use super::*;

    #[test]
    fn test_point() {
        let point_type = PointType::new(Dimension::XY, Default::default());
        let mut builder = PointBuilder::new(point_type);

        builder.push_point(Some(&Point::new(0., 1.)));
        builder.push_point(Some(&Point::new(2., 3.)));
        builder.push_point(Some(&Point::new(4., 5.)));

        let point_array: PointArray = builder.finish();
        let result = euclidean_length(&point_array).unwrap();

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
        let linestring_1 = LineString::from(vec![(0.0, 0.0), (3.0, 9.0)]);
        let linestring_2 = LineString::from(vec![(0.0, 0.0), (4.0, 5.0)]);

        let _ = linestring_builder.push_geometry(Some(&linestring_1));
        let _ = linestring_builder.push_geometry(Some(&linestring_2));
        let linestring_array = linestring_builder.finish();

        let result = euclidean_length(&linestring_array).unwrap();

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
        let linestring_1 = LineString::from(vec![(0.0, 9.0), (3.0, 4.0)]);
        let linestring_2 = LineString::from(vec![(0.0, 0.0), (4.0, 3.0)]);
        let multi_linestring_1 =
            MultiLineString::new(vec![linestring_1.clone(), linestring_2.clone()]);
        let linestring_3 = LineString::from(vec![(1.0, 5.0), (5.0, 6.0)]);
        let multi_linestring_2 = MultiLineString::new(vec![linestring_3.clone()]);

        let _ = multi_linestring_builder.push_geometry(Some(&multi_linestring_1));
        let _ = multi_linestring_builder.push_geometry(Some(&multi_linestring_2));

        let multi_linestring_array = multi_linestring_builder.finish();
        let result = euclidean_length(&multi_linestring_array).unwrap();

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
        let linestring_1 = LineString::from(vec![(0.0, 0.0), (3.0, 4.0)]);
        let linestring_2 = LineString::from(vec![(0.0, 0.0), (4.0, 5.0)]);
        let _ = wkb_builder.push_geometry(Some(&linestring_1));
        let _ = wkb_builder.push_geometry(Some(&linestring_2));
        let wkb_array = wkb_builder.finish();

        let result = euclidean_length(&wkb_array).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(result.value(0), Euclidean.length(&linestring_1));
        assert_eq!(result.value(1), Euclidean.length(&linestring_2));
    }

    #[test]
    fn test_wkb_point() {
        let mut wkb_builder: WkbBuilder<i32> =
            geoarrow_array::builder::WkbBuilder::new(WkbType::new(Default::default()));
        let point_1 = Point::new(1.0, 2.0);
        let point_2 = Point::new(3.0, 4.0);
        let _ = wkb_builder.push_geometry(Some(&point_1));
        let _ = wkb_builder.push_geometry(Some(&point_2));
        let wkb_array = wkb_builder.finish();

        let result = euclidean_length(&wkb_array).unwrap();
        assert_eq!(2, result.len());
        assert_eq!(result.value(0), 0.0);
        assert_eq!(result.value(1), 0.0);
    }

    #[test]
    fn metric_variants_linestring() {
        let mut builder = LineStringBuilder::new(
            geoarrow_schema::LineStringType::new(Dimension::XY, Default::default())
                .with_coord_type(CoordType::Separated),
        );
        let ls = LineString::from(vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]);
        let _ = builder.push_geometry(Some(&ls));
        let array = builder.finish();

        let geodesic = geodesic_length(&array).unwrap();
        let haversine = haversine_length(&array).unwrap();
        let rhumb = rhumb_length(&array).unwrap();

        assert_eq!(geodesic.value(0), Geodesic.length(&ls));
        assert_eq!(haversine.value(0), Haversine.length(&ls));
        assert_eq!(rhumb.value(0), Rhumb.length(&ls));

        // The three metric lengths are ~222 km; euclidean is ~2.0 planar degrees.
        // A metric that fell back to the planar computation fails here.
        assert!(geodesic.value(0) > 100_000.0);
        assert!(haversine.value(0) > 100_000.0);
        assert!(rhumb.value(0) > 100_000.0);
    }

    #[test]
    fn geometry_collection_sums_linear_members() {
        let ls1 = LineString::from(vec![(0.0, 0.0), (3.0, 4.0)]);
        let ls2 = LineString::from(vec![(0.0, 0.0), (0.0, 2.0)]);
        let gc = Geometry::GeometryCollection(GeometryCollection::new_from(vec![
            Geometry::LineString(ls1.clone()),
            Geometry::Point(Point::new(9.0, 9.0)),
            Geometry::LineString(ls2.clone()),
        ]));
        let array = crate::test_util::geometry_array(vec![Some(gc), None]);

        let result = euclidean_length(&array).unwrap();
        assert_eq!(
            result.value(0),
            Euclidean.length(&ls1) + Euclidean.length(&ls2)
        );
        assert!(result.is_null(1));
    }
}
