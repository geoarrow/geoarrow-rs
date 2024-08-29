use std::sync::Arc;

use geoarrow::array::{from_arrow_array, AsGeometryArray, CoordType};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::geozero::FromEWKB;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::geometry_array_to_pyobject;

/// Parse an Arrow BinaryArray from EWKB to its GeoArrow-native counterpart.
///
/// Args:
///     input: An Arrow array of Binary type holding EWKB-formatted geometries.
///
/// Returns:
///     A GeoArrow-native geometry array
#[pyfunction]
pub fn from_ewkb(input: PyArray) -> PyGeoArrowResult<PyObject> {
    let (array, field) = input.into_inner();
    let array = from_arrow_array(&array, &field)?;
    let ref_array = array.as_ref();
    let geo_array: Arc<dyn GeometryArrayTrait> = match array.data_type() {
        GeoDataType::WKB => FromEWKB::from_ewkb(
            ref_array.as_wkb(),
            CoordType::Interleaved,
            Default::default(),
            false,
        )?,
        GeoDataType::LargeWKB => FromEWKB::from_ewkb(
            ref_array.as_large_wkb(),
            CoordType::Interleaved,
            Default::default(),
            false,
        )?,
        other => {
            return Err(PyTypeError::new_err(format!("Unexpected array type {:?}", other)).into())
        }
    };
    Python::with_gil(|py| geometry_array_to_pyobject(py, geo_array))
}
