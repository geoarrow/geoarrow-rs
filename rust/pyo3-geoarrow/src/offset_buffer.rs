use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_buffer::OffsetBuffer;
use arrow_cast::cast;
use arrow_schema::DataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::PyGeoArrowError;

/// Python wrapper for an Arrow offset buffer.
///
/// Offset buffers are used in variable-length array types (like lists and strings) to mark
/// the start and end positions of each element. For GeoArrow, they're used to delineate
/// multi-part geometries like LineStrings and Polygons.
///
/// In particular, PyOffsetBuffer is used in GeoArrow constructors in `geoarrow.rust.core`.
pub struct PyOffsetBuffer(OffsetBuffer<i32>);

impl PyOffsetBuffer {
    /// Consume this wrapper and return the underlying offset buffer.
    pub fn into_inner(self) -> OffsetBuffer<i32> {
        self.0
    }
}

impl<'py> FromPyObject<'_, 'py> for PyOffsetBuffer {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
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
