use std::sync::Arc;

use crate::error::PyGeoArrowResult;
use crate::scalar::PyGeometry;
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use geoarrow::array::GeometryArrayDyn;

use geoarrow::error::GeoArrowError;
use geoarrow::scalar::GeometryScalar;
use geoarrow::trait_::GeometryArrayRef;
use geoarrow::GeometryArrayTrait;
use geozero::ProcessToJson;
use pyo3::exceptions::PyIndexError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::ffi::to_array_pycapsules;

#[pyclass(module = "geoarrow.rust.core._rust", name = "GeometryArray", subclass)]
pub struct PyGeometryArray(pub(crate) GeometryArrayDyn);

impl From<GeometryArrayDyn> for PyGeometryArray {
    fn from(value: GeometryArrayDyn) -> Self {
        Self(value)
    }
}

impl From<GeometryArrayRef> for PyGeometryArray {
    fn from(value: GeometryArrayRef) -> Self {
        Self(GeometryArrayDyn::new(value))
    }
}

impl From<PyGeometryArray> for GeometryArrayDyn {
    fn from(value: PyGeometryArray) -> Self {
        value.0
    }
}

impl PyGeometryArray {
    pub fn new(array: GeometryArrayDyn) -> Self {
        Self(array)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self.0.as_ref()
    }
}

#[pymethods]
impl PyGeometryArray {
    #[new]
    pub fn py_new(data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

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

    // /// Check for equality with other object.
    // pub fn __eq__(&self, other: &PyGeometryArray) -> bool {
    //     self.0 == other.0
    // }

    #[getter]
    pub fn __geo_interface__<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<Bound<PyAny>> {
        // Note: We create a Table out of this array so that each row can be its own Feature in a
        // FeatureCollection

        let field = self.0.extension_field();
        let geometry = self.0.to_array_ref();
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![geometry])?;

        let mut table = geoarrow::table::Table::try_new(vec![batch], schema)?;
        let json_string = table.to_json().map_err(GeoArrowError::GeozeroError)?;

        let json_mod = py.import_bound(intern!(py, "json"))?;
        let args = (json_string.into_py(py),);
        Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
    }

    pub fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
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

        Ok(Some(PyGeometry(
            GeometryScalar::try_new(self.0.slice(i, 1)).unwrap(),
        )))
    }

    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn __repr__(&self) -> String {
        self.0.to_string()
    }

    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }
}