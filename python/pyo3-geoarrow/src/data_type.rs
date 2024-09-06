use crate::error::{PyGeoArrowError, PyGeoArrowResult};

use geoarrow::datatypes::GeoDataType;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};
use pyo3_arrow::ffi::to_schema_pycapsule;
use pyo3_arrow::PyField;

#[pyclass(module = "geoarrow.rust.core._rust", name = "GeometryType", subclass)]
pub struct PyGeometryType(pub(crate) GeoDataType);

impl PyGeometryType {
    pub fn new(data_type: GeoDataType) -> Self {
        Self(data_type)
    }

    /// Import from a raw Arrow C Schema capsules
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        PyField::from_arrow_pycapsule(capsule)?.try_into()
    }

    pub fn into_inner(self) -> GeoDataType {
        self.0
    }
}

#[pymethods]
impl PyGeometryType {
    #[new]
    pub fn py_new(data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[allow(unused_variables)]
    pub fn __arrow_c_schema<'py>(&'py self, py: Python<'py>) -> PyGeoArrowResult<Bound<PyCapsule>> {
        let field = self.0.to_field("", true);
        Ok(to_schema_pycapsule(py, field)?)
    }

    /// Check for equality with other object.
    pub fn __eq__(&self, other: &PyGeometryType) -> bool {
        self.0 == other.0
    }

    pub fn __repr__(&self) -> String {
        // TODO: implement Display for GeoDataType
        format!("{:?}", self.0)
    }

    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }
}

impl From<GeoDataType> for PyGeometryType {
    fn from(value: GeoDataType) -> Self {
        Self(value)
    }
}

impl From<PyGeometryType> for GeoDataType {
    fn from(value: PyGeometryType) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyGeometryType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyField>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyField> for PyGeometryType {
    type Error = PyGeoArrowError;

    fn try_from(value: PyField) -> Result<Self, Self::Error> {
        Ok(Self(value.into_inner().as_ref().try_into()?))
    }
}
