use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyCoordType, PyDimension};

use geoarrow::datatypes::{NativeType, SerializedType};
use geoarrow_schema::{
    BoxType, GeometryCollectionType, GeometryType, LineStringType, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType, WktType,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};
use pyo3_arrow::ffi::to_schema_pycapsule;
use pyo3_arrow::PyField;

#[pyclass(module = "geoarrow.rust.core", name = "NativeType", subclass, frozen)]
pub struct PyNativeType(pub(crate) NativeType);

impl PyNativeType {
    pub fn new(data_type: NativeType) -> Self {
        Self(data_type)
    }

    /// Import from a raw Arrow C Schema capsules
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        PyField::from_arrow_pycapsule(capsule)?.try_into()
    }

    pub fn into_inner(self) -> NativeType {
        self.0
    }
}

#[allow(non_snake_case)]
#[pymethods]
impl PyNativeType {
    #[new]
    #[pyo3(signature = (r#type, dimension=None, coord_type=None))]
    fn py_new(
        r#type: &str,
        dimension: Option<PyDimension>,
        coord_type: Option<PyCoordType>,
    ) -> PyResult<Self> {
        match r#type.to_lowercase().as_str() {
            "point" => Ok(Self(NativeType::Point(PointType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "linestring" => Ok(Self(NativeType::LineString(LineStringType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "polygon" => Ok(Self(NativeType::Polygon(PolygonType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "multipoint" => Ok(Self(NativeType::MultiPoint(MultiPointType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "multilinestring" => Ok(Self(NativeType::MultiLineString(MultiLineStringType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "multipolygon" => Ok(Self(NativeType::MultiPolygon(MultiPolygonType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "geometry" => Ok(Self(NativeType::Geometry(GeometryType::new(
                coord_type.unwrap().into(),
                Default::default(),
            )))),
            "geometrycollection" => Ok(Self(NativeType::GeometryCollection(
                GeometryCollectionType::new(
                    coord_type.unwrap().into(),
                    dimension.unwrap().into(),
                    Default::default(),
                ),
            ))),
            "box" | "rect" => Ok(Self(NativeType::Rect(BoxType::new(
                dimension.unwrap().into(),
                Default::default(),
            )))),
            _ => Err(PyValueError::new_err("Unknown geometry type input")),
        }
    }

    #[allow(unused_variables)]
    fn __arrow_c_schema__<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyGeoArrowResult<Bound<'py, PyCapsule>> {
        let field = self.0.to_field("", true);
        Ok(to_schema_pycapsule(py, field)?)
    }

    /// Check for equality with other object.
    fn __eq__(&self, other: &PyNativeType) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        // TODO: implement Display for NativeType
        format!("geoarrow.rust.core.NativeType({:?})", self.0)
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, data: Self) -> Self {
        data
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[getter]
    fn coord_type(&self) -> PyCoordType {
        self.0.coord_type().into()
    }

    #[getter]
    fn dimension(&self) -> Option<PyDimension> {
        self.0.dimension().map(|d| d.into())
    }
}

impl From<NativeType> for PyNativeType {
    fn from(value: NativeType) -> Self {
        Self(value)
    }
}

impl From<PyNativeType> for NativeType {
    fn from(value: PyNativeType) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyNativeType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyField>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyField> for PyNativeType {
    type Error = PyGeoArrowError;

    fn try_from(value: PyField) -> Result<Self, Self::Error> {
        Ok(Self(value.into_inner().as_ref().try_into()?))
    }
}

#[pyclass(
    module = "geoarrow.rust.core",
    name = "SerializedType",
    subclass,
    frozen
)]
pub struct PySerializedType(pub(crate) SerializedType);

impl PySerializedType {
    pub fn new(data_type: SerializedType) -> Self {
        Self(data_type)
    }

    /// Import from a raw Arrow C Schema capsules
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        PyField::from_arrow_pycapsule(capsule)?.try_into()
    }

    pub fn into_inner(self) -> SerializedType {
        self.0
    }
}

#[allow(non_snake_case)]
#[pymethods]
impl PySerializedType {
    #[new]
    fn py_new(r#type: &str) -> PyResult<Self> {
        match r#type.to_lowercase().as_str() {
            "wkb" => Ok(Self(SerializedType::WKB(WkbType::new(Default::default())))),
            "wkt" => Ok(Self(SerializedType::WKT(WktType::new(Default::default())))),
            _ => Err(PyValueError::new_err("Unknown geometry type input")),
        }
    }

    #[allow(unused_variables)]
    fn __arrow_c_schema__<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyGeoArrowResult<Bound<'py, PyCapsule>> {
        let field = self.0.to_field("", true);
        Ok(to_schema_pycapsule(py, field)?)
    }

    /// Check for equality with other object.
    fn __eq__(&self, other: &PySerializedType) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        // TODO: implement Display for SerializedType
        format!("geoarrow.rust.core.SerializedType({:?})", self.0)
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, data: Self) -> Self {
        data
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

impl From<SerializedType> for PySerializedType {
    fn from(value: SerializedType) -> Self {
        Self(value)
    }
}

impl From<PySerializedType> for SerializedType {
    fn from(value: PySerializedType) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PySerializedType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyField>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyField> for PySerializedType {
    type Error = PyGeoArrowError;

    fn try_from(value: PyField) -> Result<Self, Self::Error> {
        Ok(Self(value.into_inner().as_ref().try_into()?))
    }
}
