pub mod geo_interface;
pub mod getitem;

use crate::error::PyGeoArrowResult;
use crate::scalar::Geometry;
use geoarrow::array::GeometryArrayDyn;

use geoarrow::scalar::GeometryScalarArray;
use geoarrow::GeometryArrayTrait;
use pyo3::exceptions::PyIndexError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

/// An immutable array of geometries using GeoArrow's in-memory representation.
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct GeometryArray(pub(crate) GeometryArrayDyn);

impl From<GeometryArrayDyn> for GeometryArray {
    fn from(value: GeometryArrayDyn) -> Self {
        Self(value)
    }
}

impl From<GeometryArray> for GeometryArrayDyn {
    fn from(value: GeometryArray) -> Self {
        value.0
    }
}

impl GeometryArray {
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self.0.as_ref()
    }
}

#[pymethods]
impl GeometryArray {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<PyTuple>> {
        let field = self.0.extension_field();
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    /// Text representation
    pub fn __repr__(&self) -> String {
        self.0.to_string()
    }

    /// Access the item at a given index
    pub fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<Geometry>> {
        // Handle negative indexes from the end
        let i = if i < 0 {
            let i = self.0.len() as isize + i;
            if i < 0 {
                return Err(PyIndexError::new_err("Index out of range").into());
            }
            i as usize
        } else {
            i as usize
        };
        if i >= self.0.len() {
            return Err(PyIndexError::new_err("Index out of range").into());
        }

        Ok(Some(Geometry(
            GeometryScalarArray::try_new(self.0.slice(i, 1)).unwrap(),
        )))
    }
}
