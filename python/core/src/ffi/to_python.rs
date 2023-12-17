use crate::array::*;
use arrow::array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use geoarrow::datatypes::GeoDataType;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::PyResult;
use std::ffi::CString;
use std::sync::Arc;

/// Implement the __arrow_c_array__ method on a GeometryArray
macro_rules! impl_arrow_c_array_geometry_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the Arrow PyCapsule Interface
            fn __arrow_c_array__(&self, _requested_schema: Option<PyObject>) -> PyResult<PyObject> {
                let field = self.0.extension_field();
                let ffi_schema = FFI_ArrowSchema::try_from(&*field).unwrap();
                let ffi_array = FFI_ArrowArray::new(&self.0.clone().into_array_ref().to_data());

                let schema_capsule_name = CString::new("arrow_schema").unwrap();
                let array_capsule_name = CString::new("arrow_array").unwrap();

                Python::with_gil(|py| {
                    let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
                    let array_capsule = PyCapsule::new(py, ffi_array, Some(array_capsule_name))?;
                    let tuple = PyTuple::new(py, vec![schema_capsule, array_capsule]);
                    Ok(tuple.to_object(py))
                })
            }
        }
    };
}

impl_arrow_c_array_geometry_array!(PointArray);
impl_arrow_c_array_geometry_array!(LineStringArray);
impl_arrow_c_array_geometry_array!(PolygonArray);
impl_arrow_c_array_geometry_array!(MultiPointArray);
impl_arrow_c_array_geometry_array!(MultiLineStringArray);
impl_arrow_c_array_geometry_array!(MultiPolygonArray);
impl_arrow_c_array_geometry_array!(MixedGeometryArray);
impl_arrow_c_array_geometry_array!(GeometryCollectionArray);
impl_arrow_c_array_geometry_array!(WKBArray);

pub fn geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn GeometryArrayTrait>,
) -> PyResult<PyObject> {
    let py_obj = match arr.data_type() {
        GeoDataType::Point(_) => PointArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::PointArray>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::LineString(_) => LineStringArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::LineStringArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::Polygon(_) => PolygonArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::PolygonArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::MultiPoint(_) => MultiPointArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::MultiPointArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::MultiLineString(_) => MultiLineStringArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::MultiLineStringArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::MultiPolygon(_) => MultiPolygonArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::MultiPolygonArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::Mixed(_) => MixedGeometryArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::MixedGeometryArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::GeometryCollection(_) => GeometryCollectionArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::GeometryCollectionArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        GeoDataType::WKB => WKBArray(
            arr.as_any()
                .downcast_ref::<geoarrow::array::WKBArray<i32>>()
                .unwrap()
                .clone(),
        )
        .into_py(py),
        other => {
            return Err(PyTypeError::new_err(format!(
                "Unexpected parsed geometry array type {:?}",
                other
            )))
        }
    };

    Ok(py_obj)
}

macro_rules! impl_arrow_c_array_primitive {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the Arrow PyCapsule Interface
            fn __arrow_c_array__(&self, _requested_schema: Option<PyObject>) -> PyResult<PyObject> {
                let ffi_schema = FFI_ArrowSchema::try_from(self.0.data_type()).unwrap();
                let ffi_array = FFI_ArrowArray::new(&self.0.to_data());

                let schema_capsule_name = CString::new("arrow_schema").unwrap();
                let array_capsule_name = CString::new("arrow_array").unwrap();

                Python::with_gil(|py| {
                    let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
                    let array_capsule = PyCapsule::new(py, ffi_array, Some(array_capsule_name))?;
                    let tuple = PyTuple::new(py, vec![schema_capsule, array_capsule]);
                    Ok(tuple.to_object(py))
                })
            }
        }
    };
}

impl_arrow_c_array_primitive!(BooleanArray);
impl_arrow_c_array_primitive!(Float16Array);
impl_arrow_c_array_primitive!(Float32Array);
impl_arrow_c_array_primitive!(Float64Array);
impl_arrow_c_array_primitive!(UInt8Array);
impl_arrow_c_array_primitive!(UInt16Array);
impl_arrow_c_array_primitive!(UInt32Array);
impl_arrow_c_array_primitive!(UInt64Array);
impl_arrow_c_array_primitive!(Int8Array);
impl_arrow_c_array_primitive!(Int16Array);
impl_arrow_c_array_primitive!(Int32Array);
impl_arrow_c_array_primitive!(Int64Array);
impl_arrow_c_array_primitive!(StringArray);
impl_arrow_c_array_primitive!(LargeStringArray);
