use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::scalar::*;
use geoarrow::array::{AsChunkedGeometryArray, AsGeometryArray};
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::datatypes::{Dimension, GeoDataType};
use geoarrow::error::GeoArrowError;
use geoarrow::GeometryArrayTrait;

use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;
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
            pub fn __arrow_c_array__<'py>(
                &'py self,
                py: Python<'py>,
                requested_schema: Option<Bound<'py, PyCapsule>>,
            ) -> PyGeoArrowResult<Bound<PyTuple>> {
                let field = self.0.extension_field();
                let array = self.0.to_array_ref();
                Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
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

pub fn geometry_to_pyobject(py: Python, geom: geoarrow::scalar::Geometry<'_, i32, 2>) -> PyObject {
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
        GeoDataType::Point(_, Dimension::XY) => {
            PointArray(arr.as_ref().as_point::<2>().clone()).into_py(py)
        }
        GeoDataType::LineString(_, Dimension::XY) => {
            LineStringArray(arr.as_ref().as_line_string::<2>().clone()).into_py(py)
        }
        GeoDataType::Polygon(_, Dimension::XY) => {
            PolygonArray(arr.as_ref().as_polygon::<2>().clone()).into_py(py)
        }
        GeoDataType::MultiPoint(_, Dimension::XY) => {
            MultiPointArray(arr.as_ref().as_multi_point::<2>().clone()).into_py(py)
        }
        GeoDataType::MultiLineString(_, Dimension::XY) => {
            MultiLineStringArray(arr.as_ref().as_multi_line_string::<2>().clone()).into_py(py)
        }
        GeoDataType::MultiPolygon(_, Dimension::XY) => {
            MultiPolygonArray(arr.as_ref().as_multi_polygon::<2>().clone()).into_py(py)
        }
        GeoDataType::Mixed(_, Dimension::XY) => {
            MixedGeometryArray(arr.as_ref().as_mixed::<2>().clone()).into_py(py)
        }
        GeoDataType::GeometryCollection(_, Dimension::XY) => {
            GeometryCollectionArray(arr.as_ref().as_geometry_collection::<2>().clone()).into_py(py)
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
        GeoDataType::Point(_, Dimension::XY) => {
            ChunkedPointArray(arr.as_ref().as_point::<2>().clone()).into_py(py)
        }
        GeoDataType::LineString(_, Dimension::XY) => {
            ChunkedLineStringArray(arr.as_ref().as_line_string::<2>().clone()).into_py(py)
        }
        GeoDataType::Polygon(_, Dimension::XY) => {
            ChunkedPolygonArray(arr.as_ref().as_polygon::<2>().clone()).into_py(py)
        }
        GeoDataType::MultiPoint(_, Dimension::XY) => {
            ChunkedMultiPointArray(arr.as_ref().as_multi_point::<2>().clone()).into_py(py)
        }
        GeoDataType::MultiLineString(_, Dimension::XY) => {
            ChunkedMultiLineStringArray(arr.as_ref().as_multi_line_string::<2>().clone())
                .into_py(py)
        }
        GeoDataType::MultiPolygon(_, Dimension::XY) => {
            ChunkedMultiPolygonArray(arr.as_ref().as_multi_polygon::<2>().clone()).into_py(py)
        }
        GeoDataType::Mixed(_, Dimension::XY) => {
            ChunkedMixedGeometryArray(arr.as_ref().as_mixed::<2>().clone()).into_py(py)
        }
        GeoDataType::GeometryCollection(_, Dimension::XY) => {
            ChunkedGeometryCollectionArray(arr.as_ref().as_geometry_collection::<2>().clone())
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
