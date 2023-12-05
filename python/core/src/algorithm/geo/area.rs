use crate::array::*;
use crate::ffi::from_python::{convert_to_geometry_array, import_arrow_c_array};
use geoarrow::algorithm::geo::Area;
use geoarrow::datatypes::GeoDataType;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

#[pyfunction]
pub fn area(ob: &PyAny) -> PyResult<Float64Array> {
    let (array, field) = import_arrow_c_array(ob)?;
    // TODO: need to improve crate's error handling
    let array = convert_to_geometry_array(&array, &field).unwrap();

    match array.data_type() {
        GeoDataType::Point(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::PointArray>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::LineString(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::LineStringArray<i32>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::LargeLineString(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::LineStringArray<i64>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::Polygon(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::PolygonArray<i32>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::LargePolygon(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::PolygonArray<i64>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::MultiPoint(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::MultiPointArray<i32>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::LargeMultiPoint(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::MultiPointArray<i64>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::MultiLineString(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::MultiLineStringArray<i32>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::LargeMultiLineString(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::MultiLineStringArray<i64>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::MultiPolygon(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::MultiPolygonArray<i32>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        GeoDataType::LargeMultiPolygon(_) => {
            let geo_arr = array
                .as_any()
                .downcast_ref::<geoarrow::array::MultiPolygonArray<i64>>()
                .unwrap();
            Ok(geo_arr.unsigned_area().into())
        }
        _ => Err(PyTypeError::new_err("Unexpected geometry type")),
    }
}

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            pub fn area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::Area;
                Area::unsigned_area(&self.0).into()
            }

            /// Signed planar area of a geometry.
            pub fn signed_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::Area;
                Area::signed_area(&self.0).into()
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);
// impl_area!(GeometryArray);
