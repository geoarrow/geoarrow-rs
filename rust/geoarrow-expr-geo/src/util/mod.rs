use std::sync::Arc;

use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_buffer::NullBuffer;
use geoarrow_array::array::{PointArray, PolygonArray};
use geoarrow_array::builder::{
    GeometryBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PointBuilder,
    PolygonBuilder,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{
    CoordType, Dimension, GeometryType, LineStringType, MultiLineStringType, MultiPolygonType,
    PointType, PolygonType,
};

use crate::util::to_geo::geometry_to_geo;

pub(crate) mod downcast;
pub mod to_geo;

pub(crate) fn copy_geoarrow_array_ref(array: &dyn GeoArrowArray) -> Arc<dyn GeoArrowArray> {
    array.slice(0, array.len())
}

/// A zero for each row, keeping the null positions of the input.
pub(crate) fn zeros(len: usize, nulls: Option<NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls)
}

/// Apply `f` to each geometry. A null row stays null.
pub(crate) fn map_to_f64<'a, F: Fn(&geo::Geometry) -> f64>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    f: F,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(f(&geo_geom));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

/// Two arrays of different lengths have no row correspondence, thus a binary op
/// must refuse them.
pub(crate) fn check_same_len(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<()> {
    if left.len() == right.len() {
        Ok(())
    } else {
        Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ))
    }
}

/// Apply `f` to each pair of geometries. A null on either side, or a `None`
/// result, gives a null row.
pub(crate) fn map_pair_to_f64<'a, F>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &'a impl GeoArrowArrayAccessor<'a>,
    f: F,
) -> GeoArrowResult<Float64Array>
where
    F: Fn(&geo::Geometry, &geo::Geometry) -> Option<f64>,
{
    let mut builder = Float64Builder::with_capacity(left.len());

    for (left, right) in left.iter().zip(right.iter()) {
        match (left, right) {
            (Some(left), Some(right)) => {
                let left = geometry_to_geo(&left?)?;
                let right = geometry_to_geo(&right?)?;
                match f(&left, &right) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            _ => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

/// `geo` is XY only, thus a Z or M input ordinate does not reach the output. A
/// `None` result gives a null row.
pub(crate) fn map_to_polygon<'a, F: Fn(&geo::Geometry) -> Option<geo::Polygon>>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    f: F,
) -> GeoArrowResult<PolygonArray> {
    let typ = PolygonType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        let polygon = match item {
            Some(geom) => f(&geometry_to_geo(&geom?)?),
            None => None,
        };
        builder.push_polygon(polygon.as_ref())?;
    }

    Ok(builder.finish())
}

/// Every builder of a concrete geometry type accepts a whole geometry, thus one
/// loop serves them all. Only the output type differs.
macro_rules! map_into_builder {
    ($array:expr, $builder:expr, $f:expr) => {{
        let array = $array;
        let mut builder = $builder;
        for item in array.iter() {
            let out = match item {
                Some(geom) => Some($f(&geometry_to_geo(&geom?)?)),
                None => None,
            };
            builder.push_geometry(out.as_ref())?;
        }
        Ok(Arc::new(builder.finish()) as Arc<dyn GeoArrowArray>)
    }};
}

/// `geo` is XY only, thus a Z or M input ordinate does not reach the output.
///
/// An array of one of the four line or areal types keeps its geometry type.
/// Every other encoding (points, collections, WKB, WKT, a mixed `Geometry`
/// array) gives a `Geometry` array.
pub(crate) fn map_geometry(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
    f: &dyn Fn(&geo::Geometry) -> geo::Geometry,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let metadata = array.data_type().metadata().clone();
    let dim = Dimension::XY;
    use geoarrow_schema::GeoArrowType::*;
    match array.data_type() {
        LineString(_) => map_into_builder!(
            array.as_line_string(),
            LineStringBuilder::new(LineStringType::new(dim, metadata).with_coord_type(coord_type)),
            f
        ),
        Polygon(_) => map_into_builder!(
            array.as_polygon(),
            PolygonBuilder::new(PolygonType::new(dim, metadata).with_coord_type(coord_type)),
            f
        ),
        MultiLineString(_) => map_into_builder!(
            array.as_multi_line_string(),
            MultiLineStringBuilder::new(
                MultiLineStringType::new(dim, metadata).with_coord_type(coord_type)
            ),
            f
        ),
        MultiPolygon(_) => map_into_builder!(
            array.as_multi_polygon(),
            MultiPolygonBuilder::new(
                MultiPolygonType::new(dim, metadata).with_coord_type(coord_type)
            ),
            f
        ),
        _ => downcast_geoarrow_array!(array, map_to_geometry_array, coord_type, f),
    }
}

fn map_to_geometry_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    f: &dyn Fn(&geo::Geometry) -> geo::Geometry,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let typ = GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    map_into_builder!(array, GeometryBuilder::new(typ), f)
}

/// `geo` is XY only, thus a Z or M input ordinate does not reach the output. A
/// `None` result gives a null row.
pub(crate) fn map_to_point<'a, F: Fn(&geo::Geometry) -> Option<geo::Point>>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    f: F,
) -> GeoArrowResult<PointArray> {
    let typ = PointType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PointBuilder::with_capacity(typ, array.len());

    for item in array.iter() {
        let point = match item {
            Some(geom) => f(&geometry_to_geo(&geom?)?),
            None => None,
        };
        builder.push_point(point.as_ref());
    }

    Ok(builder.finish())
}
