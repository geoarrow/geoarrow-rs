use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyCoordType, PyDimension};

use geoarrow::array::CoordType;
use geoarrow::datatypes::{Dimension, GeoDataType};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
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
    fn py_new(
        r#type: &str,
        coord_type: Option<PyCoordType>,
        dimension: Option<PyDimension>,
    ) -> PyResult<Self> {
        match r#type.to_lowercase().as_str() {
            "point" => Ok(Self(GeoDataType::Point(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "linestring" => Ok(Self(GeoDataType::LineString(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "polygon" => Ok(Self(GeoDataType::Polygon(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "multipoint" => Ok(Self(GeoDataType::MultiPoint(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "multilinestring" => Ok(Self(GeoDataType::MultiLineString(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "multipolygon" => Ok(Self(GeoDataType::MultiPolygon(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "geometry" => Ok(Self(GeoDataType::Mixed(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "geometrycollection" => Ok(Self(GeoDataType::GeometryCollection(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
            ))),
            "wkb" => Ok(Self(GeoDataType::WKB)),
            "box" | "rect" => Ok(Self(GeoDataType::Rect(dimension.unwrap().into()))),
            _ => Err(PyValueError::new_err("Unknown geometry type input")),
        }
    }

    #[allow(unused_variables)]
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyGeoArrowResult<Bound<PyCapsule>> {
        let field = self.0.to_field("", true);
        Ok(to_schema_pycapsule(py, field)?)
    }

    /// Check for equality with other object.
    fn __eq__(&self, other: &PyGeometryType) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        // TODO: implement Display for GeoDataType
        format!("geoarrow.rust.core.GeometryType({:?})", self.0)
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
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

    #[getter]
    fn coord_type(&self, py: Python) -> PyResult<PyObject> {
        let enums_mod = py.import_bound(intern!(py, "geoarrow.rust.core.enums"))?;
        let coord_type = enums_mod.getattr(intern!(py, "CoordType"))?;
        match self.0.coord_type() {
            None => Ok(py.None()),
            Some(CoordType::Interleaved) => {
                Ok(coord_type.getattr(intern!(py, "Interleaved"))?.into())
            }
            Some(CoordType::Separated) => Ok(coord_type.getattr(intern!(py, "Separated"))?.into()),
        }
    }

    #[getter]
    fn dimension(&self, py: Python) -> PyResult<PyObject> {
        let enums_mod = py.import_bound(intern!(py, "geoarrow.rust.core.enums"))?;
        let coord_type = enums_mod.getattr(intern!(py, "Dimension"))?;
        match self.0.dimension() {
            None => Ok(py.None()),
            Some(Dimension::XY) => Ok(coord_type.getattr(intern!(py, "XY"))?.into()),
            Some(Dimension::XYZ) => Ok(coord_type.getattr(intern!(py, "XYZ"))?.into()),
        }
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
