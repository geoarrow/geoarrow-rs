use std::sync::Arc;

use pyo3::prelude::*;

use arrow_array::Int8Array;
use arrow_array::builder::Int8Builder;
use geo_traits::{GeometryTrait, GeometryType};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{Dimension, GeoArrowType};
use pyo3_arrow::PyArray;
use pyo3_arrow::export::Arro3Array;
use pyo3_geoarrow::{PyGeoArray, PyGeoArrowResult};

#[pyfunction]
pub(crate) fn get_type_id(input: PyGeoArray) -> PyGeoArrowResult<Arro3Array> {
    let out = Arc::new(get_type_id_impl(input.inner())?);
    Ok(PyArray::from_array_ref(out).into())
}

fn get_type_id_impl(array: &dyn GeoArrowArray) -> GeoArrowResult<Int8Array> {
    let nulls = array.logical_nulls();
    let result = match array.data_type() {
        GeoArrowType::Point(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![1; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![11; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![21; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![31; array.len()].into(), nulls),
        },
        GeoArrowType::LineString(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![2; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![12; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![22; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![32; array.len()].into(), nulls),
        },
        GeoArrowType::Polygon(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![3; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![13; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![23; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![33; array.len()].into(), nulls),
        },
        GeoArrowType::Rect(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![3; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![13; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![23; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![33; array.len()].into(), nulls),
        },
        GeoArrowType::MultiPoint(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![4; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![14; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![24; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![34; array.len()].into(), nulls),
        },
        GeoArrowType::MultiLineString(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![5; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![15; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![25; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![35; array.len()].into(), nulls),
        },
        GeoArrowType::MultiPolygon(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![6; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![16; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![26; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![36; array.len()].into(), nulls),
        },
        GeoArrowType::GeometryCollection(typ) => match typ.dimension() {
            Dimension::XY => Int8Array::new(vec![7; array.len()].into(), nulls),
            Dimension::XYZ => Int8Array::new(vec![17; array.len()].into(), nulls),
            Dimension::XYM => Int8Array::new(vec![27; array.len()].into(), nulls),
            Dimension::XYZM => Int8Array::new(vec![37; array.len()].into(), nulls),
        },
        _ => downcast_geoarrow_array!(array, _get_type_id_impl)?,
    };
    Ok(result)
}

fn _get_type_id_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Int8Array> {
    let mut builder = Int8Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geom = geom?;
            let geom_offset = match geom.as_type() {
                GeometryType::Point(_) => 1,
                GeometryType::LineString(_) | GeometryType::Line(_) => 2,
                GeometryType::Polygon(_) | GeometryType::Rect(_) | GeometryType::Triangle(_) => 3,
                GeometryType::MultiPoint(_) => 4,
                GeometryType::MultiLineString(_) => 5,
                GeometryType::MultiPolygon(_) => 6,
                GeometryType::GeometryCollection(_) => 7,
            };
            let dim_offset = match geom.dim() {
                geo_traits::Dimensions::Xy => 0,
                geo_traits::Dimensions::Xyz => 10,
                geo_traits::Dimensions::Xym => 20,
                geo_traits::Dimensions::Xyzm => 30,
                geo_traits::Dimensions::Unknown(_) => unreachable!(),
            };
            builder.append_value(geom_offset + dim_offset);
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}
