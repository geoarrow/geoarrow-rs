use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::scalar::*;
use arrow::array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use geoarrow::array::{AsChunkedGeometryArray, AsGeometryArray};
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::datatypes::GeoDataType;
use geoarrow::error::GeoArrowError;
use geoarrow::GeometryArrayTrait;

use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use std::ffi::CString;
use std::sync::Arc;

/// Implement the __arrow_c_array__ method on a GeometryArray
macro_rules! impl_arrow_c_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the [Arrow PyCapsule
            /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            /// This dunder method should not be called directly, but enables zero-copy
            /// data transfer to other Python libraries that understand Arrow memory.
            ///
            /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
            /// into a pyarrow array, without copying memory.
            #[allow(unused_variables)]
            pub fn __arrow_c_array__(
                &self,
                requested_schema: Option<PyObject>,
            ) -> PyGeoArrowResult<PyObject> {
                let field = self.0.extension_field();
                let ffi_schema = FFI_ArrowSchema::try_from(&*field)?;
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

impl_arrow_c_array!(PointArray);
impl_arrow_c_array!(LineStringArray);
impl_arrow_c_array!(PolygonArray);
impl_arrow_c_array!(MultiPointArray);
impl_arrow_c_array!(MultiLineStringArray);
impl_arrow_c_array!(MultiPolygonArray);
impl_arrow_c_array!(MixedGeometryArray);
impl_arrow_c_array!(GeometryCollectionArray);
impl_arrow_c_array!(WKBArray);
impl_arrow_c_array!(RectArray);

pub fn geometry_to_pyobject(py: Python, geom: geoarrow::scalar::Geometry<'_, i32>) -> PyObject {
    match geom {
        geoarrow::scalar::Geometry::Point(g) => Point(g.into()).into_py(py),
        geoarrow::scalar::Geometry::LineString(g) => LineString(g.into()).into_py(py),
        geoarrow::scalar::Geometry::Polygon(g) => Polygon(g.into()).into_py(py),
        geoarrow::scalar::Geometry::MultiPoint(g) => MultiPoint(g.into()).into_py(py),
        geoarrow::scalar::Geometry::MultiLineString(g) => MultiLineString(g.into()).into_py(py),
        geoarrow::scalar::Geometry::MultiPolygon(g) => MultiPolygon(g.into()).into_py(py),
        geoarrow::scalar::Geometry::GeometryCollection(g) => {
            GeometryCollection(g.into()).into_py(py)
        }
        geoarrow::scalar::Geometry::Rect(g) => Rect(g.into()).into_py(py),
    }
}

pub fn geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn GeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    let py_obj = match arr.data_type() {
        GeoDataType::Point(_) => PointArray(arr.as_ref().as_point().clone()).into_py(py),
        GeoDataType::LineString(_) => {
            LineStringArray(arr.as_ref().as_line_string().clone()).into_py(py)
        }
        GeoDataType::Polygon(_) => PolygonArray(arr.as_ref().as_polygon().clone()).into_py(py),
        GeoDataType::MultiPoint(_) => {
            MultiPointArray(arr.as_ref().as_multi_point().clone()).into_py(py)
        }
        GeoDataType::MultiLineString(_) => {
            MultiLineStringArray(arr.as_ref().as_multi_line_string().clone()).into_py(py)
        }
        GeoDataType::MultiPolygon(_) => {
            MultiPolygonArray(arr.as_ref().as_multi_polygon().clone()).into_py(py)
        }
        GeoDataType::Mixed(_) => MixedGeometryArray(arr.as_ref().as_mixed().clone()).into_py(py),
        GeoDataType::GeometryCollection(_) => {
            GeometryCollectionArray(arr.as_ref().as_geometry_collection().clone()).into_py(py)
        }
        GeoDataType::WKB => WKBArray(arr.as_ref().as_wkb().clone()).into_py(py),
        other => {
            return Err(GeoArrowError::IncorrectType(
                format!("Unexpected array type {:?}", other).into(),
            )
            .into())
        }
    };

    Ok(py_obj)
}

pub fn chunked_geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn ChunkedGeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    let py_obj = match arr.data_type() {
        GeoDataType::Point(_) => ChunkedPointArray(arr.as_ref().as_point().clone()).into_py(py),
        GeoDataType::LineString(_) => {
            ChunkedLineStringArray(arr.as_ref().as_line_string().clone()).into_py(py)
        }
        GeoDataType::Polygon(_) => {
            ChunkedPolygonArray(arr.as_ref().as_polygon().clone()).into_py(py)
        }
        GeoDataType::MultiPoint(_) => {
            ChunkedMultiPointArray(arr.as_ref().as_multi_point().clone()).into_py(py)
        }
        GeoDataType::MultiLineString(_) => {
            ChunkedMultiLineStringArray(arr.as_ref().as_multi_line_string().clone()).into_py(py)
        }
        GeoDataType::MultiPolygon(_) => {
            ChunkedMultiPolygonArray(arr.as_ref().as_multi_polygon().clone()).into_py(py)
        }
        GeoDataType::Mixed(_) => {
            ChunkedMixedGeometryArray(arr.as_ref().as_mixed().clone()).into_py(py)
        }
        GeoDataType::GeometryCollection(_) => {
            ChunkedGeometryCollectionArray(arr.as_ref().as_geometry_collection().clone())
                .into_py(py)
        }
        GeoDataType::WKB => ChunkedWKBArray(arr.as_ref().as_wkb().clone()).into_py(py),
        other => {
            return Err(GeoArrowError::IncorrectType(
                format!("Unexpected array type {:?}", other).into(),
            )
            .into())
        }
    };

    Ok(py_obj)
}

macro_rules! impl_arrow_c_array_primitive {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the [Arrow PyCapsule
            /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            /// This dunder method should not be called directly, but enables zero-copy
            /// data transfer to other Python libraries that understand Arrow memory.
            ///
            /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
            /// into a pyarrow array, without copying memory.
            #[allow(unused_variables)]
            fn __arrow_c_array__(
                &self,
                requested_schema: Option<PyObject>,
            ) -> PyGeoArrowResult<PyObject> {
                let ffi_schema = FFI_ArrowSchema::try_from(self.0.data_type())?;
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
