use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::cast::AsArray;
use geoarrow::array::CoordType;
use geoarrow::io::geozero::FromWKT;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::geometry_array_to_pyobject;

#[pyfunction]
pub fn from_wkt(py: Python, input: PyArray) -> PyGeoArrowResult<PyObject> {
    let (array, _field) = input.into_inner();
    let geo_array: Arc<dyn GeometryArrayTrait> = match array.data_type() {
        DataType::Utf8 => FromWKT::from_wkt(
            array.as_string::<i32>(),
            CoordType::Interleaved,
            Default::default(),
            false,
        )?,
        DataType::LargeUtf8 => FromWKT::from_wkt(
            array.as_string::<i64>(),
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
