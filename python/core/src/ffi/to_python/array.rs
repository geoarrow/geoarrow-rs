use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::scalar::*;
use geoarrow::array::{AsChunkedGeometryArray, AsGeometryArray, GeometryArrayDyn};
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::datatypes::{Dimension, GeoDataType};
use geoarrow::error::GeoArrowError;
use geoarrow::GeometryArrayTrait;

use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;
use std::sync::Arc;

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

pub fn geometry_array_to_pyobject(py: Python, arr: Arc<dyn GeometryArrayTrait>) -> GeometryArray {
    GeometryArray(GeometryArrayDyn::new(arr))
}

pub fn chunked_geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn ChunkedGeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    use Dimension::*;
    use GeoDataType::*;

    let py_obj = match arr.data_type() {
        Point(_, XY) => ChunkedPointArray(arr.as_ref().as_point::<2>().clone()).into_py(py),
        LineString(_, XY) => {
            ChunkedLineStringArray(arr.as_ref().as_line_string::<2>().clone()).into_py(py)
        }
        Polygon(_, XY) => ChunkedPolygonArray(arr.as_ref().as_polygon::<2>().clone()).into_py(py),
        MultiPoint(_, XY) => {
            ChunkedMultiPointArray(arr.as_ref().as_multi_point::<2>().clone()).into_py(py)
        }
        MultiLineString(_, XY) => {
            ChunkedMultiLineStringArray(arr.as_ref().as_multi_line_string::<2>().clone())
                .into_py(py)
        }
        MultiPolygon(_, XY) => {
            ChunkedMultiPolygonArray(arr.as_ref().as_multi_polygon::<2>().clone()).into_py(py)
        }
        Mixed(_, XY) => ChunkedMixedGeometryArray(arr.as_ref().as_mixed::<2>().clone()).into_py(py),
        GeometryCollection(_, XY) => {
            ChunkedGeometryCollectionArray(arr.as_ref().as_geometry_collection::<2>().clone())
                .into_py(py)
        }
        WKB => ChunkedWKBArray(arr.as_ref().as_wkb().clone()).into_py(py),
        other => {
            return Err(GeoArrowError::IncorrectType(
                format!("Unexpected array type {:?}", other).into(),
            )
            .into())
        }
    };

    Ok(py_obj)
}
