use std::sync::Arc;

use geoarrow_array::array::from_arrow_array;
// use geoarrow::ArrayBase;
// use geoarrow::NativeArray;
// use geoarrow::error::GeoArrowError;
// use geoarrow::scalar::GeometryScalar;
// use geoarrow::trait_::NativeArrayRef;
use geoarrow_array::GeoArrowArray;
// use geozero::ProcessToJson;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::PyArray;
use pyo3_arrow::ffi::to_array_pycapsules;

use crate::PyGeoArrowType;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};

#[pyclass(
    module = "geoarrow.rust.core",
    name = "GeoArrowArray",
    subclass,
    frozen
)]
pub struct PyGeoArrowArray(Arc<dyn GeoArrowArray>);

impl PyGeoArrowArray {
    pub fn new(array: Arc<dyn GeoArrowArray>) -> Self {
        Self(array)
    }

    /// Import from raw Arrow capsules
    pub fn from_arrow_pycapsule(
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        PyArray::from_arrow_pycapsule(schema_capsule, array_capsule)?.try_into()
    }

    pub fn into_inner(self) -> Arc<dyn GeoArrowArray> {
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
impl PyGeoArrowArray {
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
        let field = Arc::new(self.0.data_type().to_field("", true));
        let array = self.0.to_array_ref();
        Ok(to_array_pycapsules(py, field, &array, requested_schema)?)
    }

    // /// Check for equality with other object.
    // fn __eq__(&self, other: &PyGeoArrowArray) -> bool {
    //     self.0 == other.0
    // }

    // #[getter]
    // fn __geo_interface__<'py>(&'py self, py: Python<'py>) -> PyGeoArrowResult<Bound<'py, PyAny>> {
    //     // Note: We create a Table out of this array so that each row can be its own Feature in a
    //     // FeatureCollection

    //     let field = self.0.extension_field();
    //     let geometry = self.0.to_array_ref();
    //     let schema = Arc::new(Schema::new(vec![field]));
    //     let batch = RecordBatch::try_new(schema.clone(), vec![geometry])?;

    //     let mut table = geoarrow::table::Table::try_new(vec![batch], schema)?;
    //     let json_string = table.to_json().map_err(GeoArrowError::GeozeroError)?;

    //     let json_mod = py.import(intern!(py, "json"))?;
    //     let args = (json_string,);
    //     Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
    // }

    // fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
    //     // Handle negative indexes from the end
    //     let i = if i < 0 {
    //         let i = self.0.len() as isize + i;
    //         if i < 0 {
    //             return Err(PyIndexError::new_err("Index out of range").into());
    //         }
    //         i as usize
    //     } else {
    //         i as usize
    //     };
    //     if i >= self.0.len() {
    //         return Err(PyIndexError::new_err("Index out of range").into());
    //     }

    //     Ok(Some(PyGeometry(
    //         GeometryScalar::try_new(self.0.slice(i, 1)).unwrap(),
    //     )))
    // }

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
    fn null_count(&self) -> usize {
        self.0.null_count()
    }

    #[getter]
    fn r#type(&self) -> PyGeoArrowType {
        self.0.data_type().into()
    }
}

impl From<Arc<dyn GeoArrowArray>> for PyGeoArrowArray {
    fn from(value: Arc<dyn GeoArrowArray>) -> Self {
        Self(value)
    }
}

impl From<PyGeoArrowArray> for Arc<dyn GeoArrowArray> {
    fn from(value: PyGeoArrowArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyGeoArrowArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyArray>()?.try_into()?)
    }
}

impl TryFrom<PyArray> for PyGeoArrowArray {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArray) -> Result<Self, Self::Error> {
        let (array, field) = value.into_inner();
        let geo_arr = from_arrow_array(&array, &field)?;
        Ok(Self(geo_arr))
    }
}
