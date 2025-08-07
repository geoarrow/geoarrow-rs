use std::sync::Arc;

use geoarrow_array::array::{
    CoordBuffer, GeometryArray, GeometryCollectionArray, InterleavedCoordBuffer, LineStringArray,
    MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray,
    SeparatedCoordBuffer,
};
use geoarrow_array::builder::{GeometryBuilder, GeometryCollectionBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{Dimension, GeoArrowType, GeometryType};

use crate::util::to_geo::{geometry_collection_to_geo, geometry_to_geo};

/// Force a GeoArrowArray to XY dimensions.
pub fn force_2d(array: Arc<dyn GeoArrowArray>) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;
    let out = match array.data_type() {
        Point(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_point(array.as_point())),
        },
        LineString(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_line_string(array.as_line_string())),
        },
        Polygon(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_polygon(array.as_polygon())),
        },
        MultiPoint(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_multi_point(array.as_multi_point())),
        },
        MultiLineString(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_multi_line_string(array.as_multi_line_string())),
        },
        MultiPolygon(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_multi_polygon(array.as_multi_polygon())),
        },
        Rect(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_2d_rect(array.as_rect())),
        },
        GeometryCollection(_) => Arc::new(force_2d_geometry_collection(
            array.as_geometry_collection(),
        )?),
        _ => {
            let array_ref = array.as_ref();
            Arc::new(downcast_geoarrow_array!(array_ref, force_2d_geometry_impl)?)
        }
    };
    Ok(out)
}

fn force_2d_coords(coords: &CoordBuffer) -> CoordBuffer {
    match coords {
        CoordBuffer::Interleaved(cb) => {
            let mut new_coords = Vec::with_capacity(cb.len() * 2);
            let existing_coords = cb.coords();
            match cb.dim() {
                Dimension::XY => unreachable!(),
                Dimension::XYZ | Dimension::XYM => {
                    for coord_idx in 0..cb.len() {
                        let x = existing_coords[coord_idx * 3];
                        let y = existing_coords[coord_idx * 3 + 1];
                        new_coords.push(x);
                        new_coords.push(y);
                    }
                }
                Dimension::XYZM => {
                    for coord_idx in 0..cb.len() {
                        let x = existing_coords[coord_idx * 4];
                        let y = existing_coords[coord_idx * 4 + 1];
                        new_coords.push(x);
                        new_coords.push(y);
                    }
                }
            }
            InterleavedCoordBuffer::new(new_coords.into(), Dimension::XY).into()
        }
        CoordBuffer::Separated(cb) => force_2d_separated_coords(cb).into(),
    }
}

fn force_2d_separated_coords(coords: &SeparatedCoordBuffer) -> SeparatedCoordBuffer {
    let mut buffers = coords.raw_buffers().clone();
    buffers[2] = vec![].into();
    buffers[3] = vec![].into();
    SeparatedCoordBuffer::from_array(buffers, Dimension::XY).unwrap()
}

fn force_2d_point(array: &PointArray) -> PointArray {
    PointArray::new(
        force_2d_coords(array.coords()),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_2d_line_string(array: &LineStringArray) -> LineStringArray {
    LineStringArray::new(
        force_2d_coords(array.coords()),
        array.geom_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_2d_polygon(array: &PolygonArray) -> PolygonArray {
    PolygonArray::new(
        force_2d_coords(array.coords()),
        array.geom_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_2d_multi_point(array: &MultiPointArray) -> MultiPointArray {
    MultiPointArray::new(
        force_2d_coords(array.coords()),
        array.geom_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_2d_multi_line_string(array: &MultiLineStringArray) -> MultiLineStringArray {
    MultiLineStringArray::new(
        force_2d_coords(array.coords()),
        array.geom_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_2d_multi_polygon(array: &MultiPolygonArray) -> MultiPolygonArray {
    MultiPolygonArray::new(
        force_2d_coords(array.coords()),
        array.geom_offsets().clone(),
        array.polygon_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_2d_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<GeometryArray> {
    // TODO: use with_capacity
    let mut builder = GeometryBuilder::new(GeometryType::new(array.data_type().metadata().clone()));
    for geom in array.iter() {
        if let Some(geom) = geom {
            let geom_2d = geometry_to_geo(&geom?)?;
            builder.push_geometry(Some(&geom_2d))?;
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

fn force_2d_geometry_collection(
    array: &GeometryCollectionArray,
) -> GeoArrowResult<GeometryCollectionArray> {
    // TODO: use with_capacity
    let mut builder = GeometryCollectionBuilder::new(array.extension_type().clone());
    for geom in array.iter() {
        if let Some(geom) = geom {
            let geom_2d = geometry_collection_to_geo(&geom?)?;
            builder.push_geometry_collection(Some(&geom_2d))?;
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

fn force_2d_rect(array: &RectArray) -> RectArray {
    RectArray::new(
        force_2d_separated_coords(array.lower()),
        force_2d_separated_coords(array.upper()),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use geo_traits::to_geo::{ToGeoPoint, ToGeoPolygon};
    use geoarrow_array::cast::to_wkb;
    use geoarrow_array::test::{point, polygon};
    use geoarrow_schema::CoordType;

    #[test]
    fn test_force_2d_point() {
        let array = point::array(CoordType::Separated, Dimension::XYZ);
        let array_2d = force_2d(Arc::new(array.clone())).unwrap();
        let point_array_2d = array_2d.as_point();
        let pt0 = array.value(0).unwrap().to_point();
        let pt1 = point_array_2d.value(0).unwrap().to_point();
        assert_eq!(pt0, pt1);
    }

    #[test]
    fn test_force_2d_polygon() {
        let array = polygon::array(CoordType::Separated, Dimension::XYZ);
        let array_2d = force_2d(Arc::new(array.clone())).unwrap();
        let polygon_array_2d = array_2d.as_polygon();
        let pt0 = array.value(0).unwrap().to_polygon();
        let pt1 = polygon_array_2d.value(0).unwrap().to_polygon();
        assert_eq!(pt0, pt1);
    }

    #[test]
    fn test_force_2d_wkb() {
        let array = polygon::array(CoordType::Separated, Dimension::XYZ);
        let wkb_array = to_wkb::<i32>(&array).unwrap();
        let array_2d = force_2d(Arc::new(wkb_array.clone())).unwrap();
        let geometry_array_2d = array_2d.as_geometry();
        let pt0 = geometry_to_geo(&array.value(0).unwrap()).unwrap();
        let pt1 = geometry_to_geo(&geometry_array_2d.value(0).unwrap()).unwrap();
        assert_eq!(pt0, pt1);
    }
}
