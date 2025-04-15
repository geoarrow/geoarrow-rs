use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyCoordType, PyDimension};

use geoarrow_array::GeoArrowType;
use geoarrow_schema::{
    BoxType, GeometryCollectionType, GeometryType, LineStringType, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType, WktType,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};
use pyo3_arrow::PyField;
use pyo3_arrow::ffi::to_schema_pycapsule;

#[pyclass(module = "geoarrow.rust.core", name = "GeoArrowType", subclass, frozen)]
pub struct PyGeoArrowType(pub(crate) GeoArrowType);

impl PyGeoArrowType {
    pub fn new(data_type: GeoArrowType) -> Self {
        Self(data_type)
    }

    /// Import from a raw Arrow C Schema capsules
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        PyField::from_arrow_pycapsule(capsule)?.try_into()
    }

    pub fn into_inner(self) -> GeoArrowType {
        self.0
    }
}

#[allow(non_snake_case)]
#[pymethods]
impl PyGeoArrowType {
    #[new]
    #[pyo3(signature = (r#type, dimension=None, coord_type=None))]
    fn py_new(
        r#type: &str,
        dimension: Option<PyDimension>,
        coord_type: Option<PyCoordType>,
    ) -> PyResult<Self> {
        match r#type.to_lowercase().as_str() {
            "point" => Ok(Self(GeoArrowType::Point(PointType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "linestring" => Ok(Self(GeoArrowType::LineString(LineStringType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "polygon" => Ok(Self(GeoArrowType::Polygon(PolygonType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "multipoint" => Ok(Self(GeoArrowType::MultiPoint(MultiPointType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "multilinestring" => Ok(Self(GeoArrowType::MultiLineString(
                MultiLineStringType::new(
                    coord_type.unwrap().into(),
                    dimension.unwrap().into(),
                    Default::default(),
                ),
            ))),
            "multipolygon" => Ok(Self(GeoArrowType::MultiPolygon(MultiPolygonType::new(
                coord_type.unwrap().into(),
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "geometry" => Ok(Self(GeoArrowType::Geometry(GeometryType::new(
                coord_type.unwrap().into(),
                Default::default(),
            )))),
            "geometrycollection" => Ok(Self(GeoArrowType::GeometryCollection(
                GeometryCollectionType::new(
                    coord_type.unwrap().into(),
                    dimension.unwrap().into(),
                    Default::default(),
                ),
            ))),
            "box" | "rect" => Ok(Self(GeoArrowType::Rect(BoxType::new(
                dimension.unwrap().into(),
                Default::default(),
            )))),
            "wkb" => Ok(Self(GeoArrowType::Wkb(WkbType::new(Default::default())))),
            "wkt" => Ok(Self(GeoArrowType::Wkt(WktType::new(Default::default())))),
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
    fn __eq__(&self, other: &PyGeoArrowType) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        // TODO: implement Display for GeoArrowType
        format!("geoarrow.rust.core.GeoArrowType({:?})", self.0)
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
    fn coord_type(&self) -> Option<PyCoordType> {
        self.0.coord_type().map(|c| c.into())
    }

    #[getter]
    fn dimension(&self) -> Option<PyDimension> {
        self.0.dimension().map(|d| d.into())
    }
}

impl From<GeoArrowType> for PyGeoArrowType {
    fn from(value: GeoArrowType) -> Self {
        Self(value)
    }
}

impl From<PyGeoArrowType> for GeoArrowType {
    fn from(value: PyGeoArrowType) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyGeoArrowType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        ob.extract::<PyField>()?.try_into().map_err(PyErr::from)
    }
}

impl TryFrom<PyField> for PyGeoArrowType {
    type Error = PyGeoArrowError;

    fn try_from(value: PyField) -> Result<Self, Self::Error> {
        Ok(Self(value.into_inner().as_ref().try_into()?))
    }
}
