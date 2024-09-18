use std::sync::Arc;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyGeometry, PyGeometryType};
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use geoarrow::array::{from_arrow_array, GeometryArrayDyn};

use geoarrow::error::GeoArrowError;
use geoarrow::scalar::GeometryScalar;
use geoarrow::trait_::NativeArrayRef;
use geoarrow::NativeArray;
use geozero::ProcessToJson;
use pyo3::exceptions::PyIndexError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::ffi::to_array_pycapsules;
use pyo3_arrow::PyArray;

#[pyclass(module = "geoarrow.rust.core._rust", name = "GeometryArray", subclass)]
pub struct PyGeometryArray(pub(crate) GeometryArrayDyn);

impl PyGeometryArray {
    pub fn new(array: GeometryArrayDyn) -> Self {
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

    pub fn into_inner(self) -> GeometryArrayDyn {
        self.0
    }

    /// Export to a geoarrow.rust.core.GeometryArray.
    ///
    /// This requires that you depend on geoarrow-rust-core from your Python package.
    pub fn to_geoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let geoarrow_mod = py.import_bound(intern!(py, "geoarrow.rust.core"))?;
        geoarrow_mod
            .getattr(intern!(py, "GeometryArray"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                self.__arrow_c_array__(py, None)?,
            )
    }
}

#[pymethods]
impl PyGeometryArray {
    #[new]
    fn py_new(data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[allow(unused_variables)]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<PyTuple>> {
        let field = self.0.extension_field();
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    // /// Check for equality with other object.
    // fn __eq__(&self, other: &PyGeometryArray) -> bool {
    //     self.0 == other.0
    // }

    #[getter]
    fn __geo_interface__<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<Bound<PyAny>> {
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
        "geoarrow.rust.core.GeometryArray".to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
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
    fn r#type(&self) -> PyGeometryType {
        self.0.data_type().into()
    }
}

impl From<GeometryArrayDyn> for PyGeometryArray {
    fn from(value: GeometryArrayDyn) -> Self {
        Self(value)
    }
}

impl From<NativeArrayRef> for PyGeometryArray {
    fn from(value: NativeArrayRef) -> Self {
        Self(GeometryArrayDyn::new(value))
    }
}

impl From<PyGeometryArray> for GeometryArrayDyn {
    fn from(value: PyGeometryArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyGeometryArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyArray>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyArray> for PyGeometryArray {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArray) -> Result<Self, Self::Error> {
        let (array, field) = value.into_inner();
        let geo_array = from_arrow_array(&array, &field)?;
        Ok(Self(geo_array.into()))
    }
}
