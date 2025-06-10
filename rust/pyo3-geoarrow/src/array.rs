use std::sync::Arc;

use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_cast::downcast::NativeType;
use geoarrow_schema::{
    BoxType, GeometryCollectionType, LineStringType, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};
use pyo3::exceptions::PyIndexError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::PyArray;
use pyo3_arrow::ffi::to_array_pycapsules;

use crate::PyCoordType;
use crate::data_type::PyGeoArrowType;
use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::scalar::PyGeoArrowScalar;

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

    pub fn inner(&self) -> &Arc<dyn GeoArrowArray> {
        &self.0
    }

    pub fn into_inner(self) -> Arc<dyn GeoArrowArray> {
        self.0
    }

    /// Export to a geoarrow.rust.core.GeoArrowArray.
    ///
    /// This requires that you depend on geoarrow-rust-core from your Python package.
    pub fn to_geoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let geoarrow_mod = py.import(intern!(py, "geoarrow.rust.core"))?;
        geoarrow_mod
            .getattr(intern!(py, "GeoArrowArray"))?
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

    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        // Do extraction within body because `__eq__` should never raise an exception.
        if let Ok(other) = other.extract::<Self>() {
            self.0.data_type() == other.0.data_type()
                && self.0.to_array_ref() == other.0.to_array_ref()
        } else {
            false
        }
    }

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

    fn __getitem__(&self, i: isize) -> PyGeoArrowResult<PyGeoArrowScalar> {
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

        PyGeoArrowScalar::try_new(self.0.slice(i, 1))
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        "GeoArrowArray".to_string()
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
        self.0.logical_null_count()
    }

    #[pyo3(signature = (to_type, /))]
    fn cast(&self, to_type: PyGeoArrowType) -> PyGeoArrowResult<Self> {
        let casted = geoarrow_cast::cast::cast(self.0.as_ref(), &to_type.into_inner())?;
        Ok(Self(casted))
    }

    #[pyo3(
        signature = (*, coord_type = PyCoordType::Separated),
        text_signature = "(*, coord_type='separated')"
    )]
    fn downcast(&self, coord_type: PyCoordType) -> PyGeoArrowResult<Self> {
        if let Some((native_type, dim)) =
            geoarrow_cast::downcast::infer_downcast_type(std::iter::once(self.0.as_ref()))?
        {
            let metadata = self.0.data_type().metadata().clone();
            let coord_type = coord_type.into();
            let to_type = match native_type {
                NativeType::Point => PointType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::LineString => LineStringType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::Polygon => PolygonType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::MultiPoint => MultiPointType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::MultiLineString => MultiLineStringType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::MultiPolygon => MultiPolygonType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::GeometryCollection => GeometryCollectionType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into(),
                NativeType::Rect => BoxType::new(dim, metadata).into(),
            };
            self.cast(PyGeoArrowType::new(to_type))
        } else {
            Ok(Self::new(self.0.clone()))
        }
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
