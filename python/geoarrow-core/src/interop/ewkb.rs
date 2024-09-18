use std::sync::Arc;

use geoarrow::array::{from_arrow_array, AsNativeArray, CoordType};
use geoarrow::datatypes::NativeType;
use geoarrow::io::geozero::FromEWKB;
use geoarrow::NativeArray;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::ffi::to_python::geometry_array_to_pyobject;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn from_ewkb(py: Python, input: PyArray) -> PyGeoArrowResult<PyObject> {
    let (array, field) = input.into_inner();
    let array = from_arrow_array(&array, &field)?;
    let ref_array = array.as_ref();
    let geo_array: Arc<dyn NativeArray> = match array.data_type() {
        NativeType::WKB => FromEWKB::from_ewkb(
            ref_array.as_wkb(),
            CoordType::Interleaved,
            Default::default(),
            false,
        )?,
        NativeType::LargeWKB => FromEWKB::from_ewkb(
            ref_array.as_large_wkb(),
            CoordType::Interleaved,
            Default::default(),
            false,
        )?,
        other => {
            return Err(PyTypeError::new_err(format!("Unexpected array type {:?}", other)).into())
        }
    };
    geometry_array_to_pyobject(py, geo_array)
}
