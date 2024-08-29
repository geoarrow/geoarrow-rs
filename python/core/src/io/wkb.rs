use std::sync::Arc;

use geoarrow::array::{AsGeometryArray, CoordType, GeometryArrayDyn};
use geoarrow::datatypes::GeoDataType;
use geoarrow::error::GeoArrowError;
use geoarrow::io::wkb::{to_wkb as _to_wkb, FromWKB};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

use crate::array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::geometry_array_to_pyobject;

/// Parse an Arrow BinaryArray from WKB to its GeoArrow-native counterpart.
///
/// This expects ISO-formatted WKB geometries.
///
/// Args:
///     input: An Arrow array of Binary type holding WKB-formatted geometries.
///
/// Returns:
///     A GeoArrow-native geometry array
#[pyfunction]
pub fn from_wkb(input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let geo_array: Arc<dyn GeometryArrayTrait> = match arr.data_type() {
                GeoDataType::WKB => {
                    FromWKB::from_wkb(arr.as_ref().as_wkb(), CoordType::Interleaved)?
                }
                GeoDataType::LargeWKB => {
                    FromWKB::from_wkb(arr.as_ref().as_large_wkb(), CoordType::Interleaved)?
                }
                other => {
                    return Err(GeoArrowError::IncorrectType(
                        format!("Unexpected array type {:?}", other).into(),
                    )
                    .into())
                }
            };
            Python::with_gil(|py| geometry_array_to_pyobject(py, geo_array))
        }
        AnyGeometryInput::Chunked(_) => todo!(),
    }
}

/// Encode a GeoArrow-native geometry array to a WKBArray, holding ISO-formatted WKB geometries.
///
/// Args:
///     input: A GeoArrow-native geometry array
///
/// Returns:
///     An array with WKB-formatted geometries
#[pyfunction]
pub fn to_wkb(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(
            _to_wkb::<i32>(arr.as_ref()),
        )))
        .into_py(py)),
        AnyGeometryInput::Chunked(_) => todo!(),
    }
}
