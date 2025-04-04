use std::sync::Arc;

use crate::data_type::PySerializedType;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyGeometry, PyNativeType};
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use geoarrow::array::{NativeArrayDyn, SerializedArray, SerializedArrayDyn};
use geoarrow::error::GeoArrowError;
use geoarrow::scalar::GeometryScalar;
use geoarrow::trait_::NativeArrayRef;
use geoarrow::ArrayBase;
use geoarrow::NativeArray;
use geozero::ProcessToJson;
use pyo3::exceptions::PyIndexError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::ffi::to_array_pycapsules;
use pyo3_arrow::PyArray;

#[pyclass(module = "geoarrow.rust.core", name = "NativeArray", subclass, frozen)]
pub struct PyNativeArray(pub(crate) NativeArrayDyn);

impl PyNativeArray {
    pub fn new(array: NativeArrayDyn) -> Self {
        Self(array)
    }

    /// Import from raw Arrow capsules
    pub fn from_arrow_pycapsule(
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        PyArray::from_arrow_pycapsule(schema_capsule, array_capsule)?.try_into()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn NativeArray {
        self.0.as_ref()
    }

    pub fn into_inner(self) -> NativeArrayDyn {
        self.0
    }

    /// Export to a geoarrow.rust.core.NativeArray.
    ///
    /// This requires that you depend on geoarrow-rust-core from your Python package.
    pub fn to_geoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let geoarrow_mod = py.import(intern!(py, "geoarrow.rust.core"))?;
        geoarrow_mod
            .getattr(intern!(py, "NativeArray"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                self.__arrow_c_array__(py, None)?,
            )
    }
}

#[pymethods]
impl PyNativeArray {
    #[new]
    fn py_new(data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<'py, PyTuple>> {
        let field = self.0.extension_field();
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    // /// Check for equality with other object.
    // fn __eq__(&self, other: &PyNativeArray) -> bool {
    //     self.0 == other.0
    // }

    #[getter]
    fn __geo_interface__<'py>(&'py self, py: Python<'py>) -> PyGeoArrowResult<Bound<'py, PyAny>> {
        // Note: We create a Table out of this array so that each row can be its own Feature in a
        // FeatureCollection

        let field = self.0.extension_field();
        let geometry = self.0.to_array_ref();
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![geometry])?;

        let mut table = geoarrow::table::Table::try_new(vec![batch], schema)?;
        let json_string = table.to_json().map_err(GeoArrowError::GeozeroError)?;

        let json_mod = py.import(intern!(py, "json"))?;
        let args = (json_string,);
        Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
    }

    fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
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

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        "geoarrow.rust.core.NativeArray".to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, data: Self) -> Self {
        data
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        Self::from_arrow_pycapsule(schema_capsule, array_capsule)
    }

    #[getter]
    fn r#type(&self) -> PyNativeType {
        self.0.data_type().into()
    }
}

impl From<NativeArrayDyn> for PyNativeArray {
    fn from(value: NativeArrayDyn) -> Self {
        Self(value)
    }
}

impl From<NativeArrayRef> for PyNativeArray {
    fn from(value: NativeArrayRef) -> Self {
        Self(NativeArrayDyn::new(value))
    }
}

impl From<PyNativeArray> for NativeArrayDyn {
    fn from(value: PyNativeArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyNativeArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyArray>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyArray> for PyNativeArray {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArray) -> Result<Self, Self::Error> {
        let (array, field) = value.into_inner();
        Ok(Self(NativeArrayDyn::from_arrow_array(&array, &field)?))
    }
}

#[pyclass(
    module = "geoarrow.rust.core._rust",
    name = "SerializedArray",
    subclass,
    frozen
)]
pub struct PySerializedArray(pub(crate) SerializedArrayDyn);

impl PySerializedArray {
    pub fn new(array: SerializedArrayDyn) -> Self {
        Self(array)
    }

    /// Import from raw Arrow capsules
    pub fn from_arrow_pycapsule(
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        PyArray::from_arrow_pycapsule(schema_capsule, array_capsule)?.try_into()
    }
}

#[pymethods]
impl PySerializedArray {
    #[new]
    fn py_new(data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<'py, PyTuple>> {
        let field = self.0.extension_field();
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        "geoarrow.rust.core.SerializedArray".to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, data: Self) -> Self {
        data
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        Self::from_arrow_pycapsule(schema_capsule, array_capsule)
    }

    #[getter]
    fn r#type(&self) -> PySerializedType {
        self.0.data_type().into()
    }
}

impl<'a> FromPyObject<'a> for PySerializedArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyArray>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyArray> for PySerializedArray {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArray) -> Result<Self, Self::Error> {
        let (array, field) = value.into_inner();
        Ok(Self(SerializedArrayDyn::from_arrow_array(&array, &field)?))
    }
}
