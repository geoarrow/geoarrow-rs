use std::sync::Arc;

use geoarrow_array::array::{
    GeometryArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
};
use geoarrow_array::builder::{LineStringBuilder, PointBuilder, PolygonBuilder};
use geoarrow_array::capacity::{LineStringCapacity, PolygonCapacity};
use geoarrow_array::cast::{AsGeoArrowArray, from_wkb, from_wkt, to_wkb, to_wkt};
use geoarrow_array::error::{GeoArrowError, Result};
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};

/// Cast a `GeoArrowArray` to another `GeoArrowType`.
///
/// ### Criteria:
///
/// - Dimension must be compatible:
///     - If the source array and destination type are both dimension-aware, then their dimensions
///       must match.
///     - Casts to dimensionless arrays are always allowed.
///     - Casts from dimensionless arrays to dimension-aware arrays are never allowed.
/// - GeoArrow Metadata must match.
/// - Only supports infallible casts. E.g. `Point` to `MultiPoint`, `LineString` to
///   `MultiLineString`, etc. But not `MultiPoint` to `Point`, etc. Those need to be aware of
///   potentially multiple batches of arrays. Whereas this `cast` can be applied in isolation to
///   multiple batches of a chunked array.
///
/// ### Infallible casts:
///
/// As long as the above criteria are met, these casts will always succeed without erroring.
///
/// - The same type with different coord types.
/// - Any source array type to `Geometry`, `Wkb`, `LargeWkb`, `Wkt`, or `LargeWkt`.
/// - `Point` to `MultiPoint`
/// - `LineString` to `MultiLineString`
/// - `Polygon` to `MultiPolygon`
///
/// ### Fallible casts:
///
/// - `Geometry` to any other native type.
/// - Parsing `WKB` or `WKT` to any native type other than `Geometry`.
/// - `MultiPoint` to `Point`
/// - `MultiLineString` to `LineString`
/// - `MultiPolygon` to `Polygon`
///
pub fn cast(array: &dyn GeoArrowArray, to_type: &GeoArrowType) -> Result<Arc<dyn GeoArrowArray>> {
    // We want to error if the dimensions aren't compatible, but allow conversions to
    // `GeometryArray`, `WKB`, etc where the target array isn't parameterized by a specific
    // dimension.
    match (array.data_type().dimension(), to_type.dimension()) {
        (Some(from_dim), Some(to_dim)) => {
            if from_dim != to_dim {
                return Err(GeoArrowError::General(format!(
                    "Cannot cast from {:?} to {:?}: incompatible dimensions",
                    from_dim, to_dim
                )));
            }
        }
        (None, None) => {}
        (Some(_), None) => {}
        (None, Some(_)) => {
            return Err(GeoArrowError::General(
                "Cannot infallibly cast from a dimension-less array to a dimension-aware array."
                    .to_string(),
            ));
        }
    };

    if array.data_type().metadata() != to_type.metadata() {
        return Err(GeoArrowError::General(format!(
            "Cannot cast from {:?} to {:?}: incompatible metadata",
            array.data_type().metadata(),
            to_type.metadata(),
        )));
    }

    use GeoArrowType::*;
    let out: Arc<dyn GeoArrowArray> = match (array.data_type(), to_type) {
        (Point(_), Point(to_type)) => {
            let array = array.as_point();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (Point(_), MultiPoint(to_type)) => {
            let mp_array = MultiPointArray::from(array.as_point().clone());
            Arc::new(mp_array.into_coord_type(to_type.coord_type()))
        }
        (Point(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_point().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (LineString(_), LineString(to_type)) => {
            let array = array.as_line_string();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (LineString(_), MultiLineString(to_type)) => {
            let mp_array = MultiLineStringArray::from(array.as_line_string().clone());
            Arc::new(mp_array.into_coord_type(to_type.coord_type()))
        }
        (LineString(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_line_string().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (Polygon(_), Polygon(to_type)) => {
            let array = array.as_polygon();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (Polygon(_), MultiPolygon(to_type)) => {
            let mp_array = MultiPolygonArray::from(array.as_polygon().clone());
            Arc::new(mp_array.into_coord_type(to_type.coord_type()))
        }
        (Polygon(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_polygon().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (MultiPoint(_), Point(to_type)) => {
            let mut builder = PointBuilder::with_capacity(to_type.clone(), array.len());
            for geom in array.as_multi_point().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (MultiPoint(_), MultiPoint(to_type)) => {
            let array = array.as_multi_point();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (MultiPoint(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_multi_point().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (MultiLineString(_), LineString(to_type)) => {
            let ml_array = array.as_multi_line_string();
            let ml_capacity = ml_array.buffer_lengths();
            let ls_capacity =
                LineStringCapacity::new(ml_capacity.coord_capacity(), ml_capacity.geom_capacity());
            let mut builder = LineStringBuilder::with_capacity(to_type.clone(), ls_capacity);
            for geom in array.as_multi_line_string().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (MultiLineString(_), MultiLineString(to_type)) => {
            let array = array.as_multi_line_string();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (MultiLineString(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_multi_line_string().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (MultiPolygon(_), Polygon(to_type)) => {
            let mp_array = array.as_multi_polygon();
            let mp_capacity = mp_array.buffer_lengths();
            let p_capacity = PolygonCapacity::new(
                mp_capacity.coord_capacity(),
                mp_capacity.ring_capacity(),
                mp_capacity.geom_capacity(),
            );
            let mut builder = PolygonBuilder::with_capacity(to_type.clone(), p_capacity);
            for geom in mp_array.iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (MultiPolygon(_), MultiPolygon(to_type)) => {
            let array = array.as_multi_polygon();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (MultiPolygon(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_multi_polygon().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (Geometry(_), Geometry(to_type)) => {
            let array = array.as_geometry();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (GeometryCollection(_), GeometryCollection(to_type)) => {
            let array = array.as_geometry_collection();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (GeometryCollection(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_geometry_collection().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (_, Wkb(_)) => Arc::new(to_wkb::<i32>(array)?),
        (_, LargeWkb(_)) => Arc::new(to_wkb::<i64>(array)?),
        (_, Wkt(_)) => Arc::new(to_wkt::<i32>(array)?),
        (_, LargeWkt(_)) => Arc::new(to_wkt::<i64>(array)?),
        (Wkb(_), _) => from_wkb(array.as_wkb::<i32>(), to_type.clone(), false)?,
        (LargeWkb(_), _) => from_wkb(array.as_wkb::<i64>(), to_type.clone(), false)?,
        (Wkt(_), _) => from_wkt(array.as_wkt::<i32>(), to_type.clone(), false)?,
        (LargeWkt(_), _) => from_wkt(array.as_wkt::<i64>(), to_type.clone(), false)?,
        (_, _) => {
            return Err(GeoArrowError::General(format!(
                "Unsupported cast from {:?} to {:?}",
                array.data_type(),
                to_type
            )));
        }
    };
    Ok(out)
}

#[cfg(test)]
mod test {
    use geoarrow_array::builder::MultiPointBuilder;
    use geoarrow_array::{IntoArrow, test};
    use geoarrow_schema::{CoordType, Dimension, GeometryType, MultiPointType, PointType, WkbType};
    use wkt::wkt;

    use super::*;

    #[test]
    fn test_point() {
        let array = test::point::array(CoordType::Interleaved, Dimension::XY);

        // Cast to the same type
        let array2 = cast(&array, &array.data_type()).unwrap();
        assert_eq!(&array, array2.as_point());

        // Cast to other coord type
        let p_type = PointType::new(
            CoordType::Separated,
            Dimension::XY,
            array.data_type().metadata().clone(),
        );
        let array3 = cast(&array, &p_type.into()).unwrap();
        assert_eq!(
            array3.as_point().ext_type().coord_type(),
            CoordType::Separated
        );

        // Cast to multi point
        let mp_type = MultiPointType::new(
            CoordType::Interleaved,
            Dimension::XY,
            array.data_type().metadata().clone(),
        );
        let mp_array = cast(&array, &mp_type.into()).unwrap();
        assert!(mp_array.as_multi_point_opt().is_some());

        // Cast to geometry
        let mp_type =
            GeometryType::new(CoordType::Interleaved, array.data_type().metadata().clone());
        let mp_array = cast(&array, &mp_type.into()).unwrap();
        assert!(mp_array.as_geometry_opt().is_some());
    }

    #[test]
    fn cast_to_wkb() {
        let array = test::point::array(CoordType::Interleaved, Dimension::XY);

        let wkb_type = GeoArrowType::Wkb(WkbType::new(array.data_type().metadata().clone()));
        let wkb_array = cast(&array, &wkb_type).unwrap();
        assert!(wkb_array.as_wkb_opt::<i32>().is_some());

        let large_wkb_type =
            GeoArrowType::LargeWkb(WkbType::new(array.data_type().metadata().clone()));
        let wkb_array = cast(&array, &large_wkb_type).unwrap();
        assert!(wkb_array.as_wkb_opt::<i64>().is_some());
    }

    #[test]
    fn downcast_single_multi_points() {
        let mp1 = wkt! { MULTIPOINT(0.0 0.0) };
        let mp2 = wkt! { MULTIPOINT(1.0 2.0) };
        let mp3 = wkt! { MULTIPOINT(3.0 4.0) };

        let typ = MultiPointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
        let mp_arr = MultiPointBuilder::from_multi_points(&[mp1, mp2, mp3], typ).finish();
        let (coord_type, dim, metadata) = mp_arr.ext_type().clone().into_inner();
        let p_type = PointType::new(coord_type, dim, metadata);
        let p_arr = cast(&mp_arr, &p_type.into()).unwrap();
        assert!(p_arr.as_point_opt().is_some());
    }
}
