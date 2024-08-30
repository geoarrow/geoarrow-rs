use crate::array::*;
use geoarrow::array::{from_arrow_array, GeometryArrayDyn};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};
use pyo3_arrow::PyArray;

impl<'a> FromPyObject<'a> for PyGeometryArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let (array, field) = ob.extract::<PyArray>()?.into_inner();
        let geo_arr = from_arrow_array(&array, &field)
            .map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self(GeometryArrayDyn::new(geo_arr)))
    }
}
