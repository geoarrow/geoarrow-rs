use arrow::datatypes::{Float64Type, Int32Type};
use geoarrow::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::ffi::from_python::input::PyScalarBuffer;

#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct CoordBuffer(pub(crate) geoarrow::array::CoordBuffer);

#[pymethods]
impl CoordBuffer {
    /// Construct a CoordBuffer from a single buffer of interleaved XY coordinates.
    #[classmethod]
    pub fn from_interleaved(_cls: &PyType, coords: PyScalarBuffer<Float64Type>) -> Self {
        let coord_buffer = InterleavedCoordBuffer::new(coords.0);
        Self(geoarrow::array::CoordBuffer::Interleaved(coord_buffer))
    }

    /// Construct a CoordBuffer from two buffers of separated X and Y coordinates.
    #[classmethod]
    pub fn from_separated(
        _cls: &PyType,
        x: PyScalarBuffer<Float64Type>,
        y: PyScalarBuffer<Float64Type>,
    ) -> Self {
        let coord_buffer = SeparatedCoordBuffer::new(x.0, y.0);
        Self(geoarrow::array::CoordBuffer::Separated(coord_buffer))
    }
}

impl From<geoarrow::array::CoordBuffer> for CoordBuffer {
    fn from(value: geoarrow::array::CoordBuffer) -> Self {
        Self(value)
    }
}

impl From<CoordBuffer> for geoarrow::array::CoordBuffer {
    fn from(value: CoordBuffer) -> Self {
        value.0
    }
}

#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct OffsetBuffer(pub(crate) arrow_buffer::buffer::OffsetBuffer<i32>);

impl<'a> FromPyObject<'a> for OffsetBuffer {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let scalar_buffer = ob.extract::<PyScalarBuffer<Int32Type>>()?;
        Ok(Self(arrow_buffer::buffer::OffsetBuffer::new(
            scalar_buffer.0,
        )))
    }
}
