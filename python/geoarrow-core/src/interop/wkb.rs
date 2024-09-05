use std::sync::Arc;

use geoarrow::array::{AsChunkedGeometryArray, AsGeometryArray, GeometryArrayDyn};
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::datatypes::GeoDataType;
use geoarrow::error::GeoArrowError;
use geoarrow::io::wkb::{to_wkb as _to_wkb, FromWKB, ToWKB};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

use crate::array::*;
use crate::coord_type::PyCoordType;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};

#[pyfunction]
#[pyo3(
    signature = (input, *, coord_type = PyCoordType::Interleaved),
    text_signature = "(input, *, method = 'interleaved')")
]
pub fn from_wkb(
    py: Python,
    input: AnyGeometryInput,
    coord_type: PyCoordType,
) -> PyGeoArrowResult<PyObject> {
    let coord_type = coord_type.into();
    match input {
        AnyGeometryInput::Array(arr) => {
            let geo_array: Arc<dyn GeometryArrayTrait> = match arr.0.data_type() {
                GeoDataType::WKB => FromWKB::from_wkb(arr.as_ref().as_wkb(), coord_type)?,
                GeoDataType::LargeWKB => {
                    FromWKB::from_wkb(arr.as_ref().as_large_wkb(), coord_type)?
                }
                other => {
                    return Err(GeoArrowError::IncorrectType(
                        format!("Unexpected array type {:?}", other).into(),
                    )
                    .into())
                }
            };
            geometry_array_to_pyobject(py, geo_array)
        }
        AnyGeometryInput::Chunked(s) => {
            let geo_array: Arc<dyn ChunkedGeometryArrayTrait> = match s.0.data_type() {
                GeoDataType::WKB => FromWKB::from_wkb(s.as_ref().as_wkb(), coord_type)?,
                GeoDataType::LargeWKB => FromWKB::from_wkb(s.as_ref().as_large_wkb(), coord_type)?,
                other => {
                    return Err(GeoArrowError::IncorrectType(
                        format!("Unexpected array type {:?}", other).into(),
                    )
                    .into())
                }
            };
            chunked_geometry_array_to_pyobject(py, geo_array)
        }
    }
}

#[pyfunction]
pub fn to_wkb(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(
            _to_wkb::<i32>(arr.as_ref()),
        )))
        .into_py(py)),
        AnyGeometryInput::Chunked(s) => {
            let out = s.0.as_ref().to_wkb::<i32>();
            chunked_geometry_array_to_pyobject(py, Arc::new(out))
        }
    }
}
