use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::Int32Type;
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::PyGeoArrowError;

pub struct PyOffsetBuffer(OffsetBuffer<i32>);

impl PyOffsetBuffer {
    pub fn into_inner(self) -> OffsetBuffer<i32> {
        self.0
    }
}

impl<'py> FromPyObject<'py> for PyOffsetBuffer {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let ob = ob.extract::<PyArray>()?;
        if ob.array().null_count() != 0 {
            return Err(PyValueError::new_err(format!(
                "Cannot construct offset buffer with nulls. Got {} nulls.",
                ob.array().null_count()
            )));
        }
        let offsets = cast(ob.as_ref(), &DataType::Int32).map_err(PyGeoArrowError::from)?;
        let offsets = offsets.as_ref().as_primitive::<Int32Type>();
        Ok(Self(OffsetBuffer::new(offsets.values().clone())))
    }
}
