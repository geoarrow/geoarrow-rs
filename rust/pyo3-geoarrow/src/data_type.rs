use std::sync::Arc;

use crate::error::{PyGeoArrowError, PyGeoArrowResult};
use crate::{PyCoordType, PyCrs, PyDimension, PyEdges};

use geoarrow_array::GeoArrowType;
use geoarrow_schema::{
    BoxType, GeometryCollectionType, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType, WktType,
};
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
    #[allow(unused_variables)]
    fn __arrow_c_schema__<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyGeoArrowResult<Bound<'py, PyCapsule>> {
        let field = self.0.to_field("", true);
        Ok(to_schema_pycapsule(py, field)?)
    }

    /// Check for equality with other object.
    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        // Do extraction within body because `__eq__` should never raise an exception.
        if let Ok(other) = other.extract::<Self>() {
            self.0 == other.0
        } else {
            false
        }
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

    #[getter]
    fn crs(&self) -> PyCrs {
        self.0.metadata().crs().clone().into()
    }

    #[getter]
    fn edges(&self) -> Option<PyEdges> {
        self.0.metadata().edges().map(|e| e.into())
    }
}

impl AsRef<GeoArrowType> for PyGeoArrowType {
    fn as_ref(&self) -> &GeoArrowType {
        &self.0
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
        Ok(ob.extract::<PyField>()?.try_into()?)
    }
}

impl TryFrom<PyField> for PyGeoArrowType {
    type Error = PyGeoArrowError;

    fn try_from(value: PyField) -> Result<Self, Self::Error> {
        Ok(Self(value.into_inner().as_ref().try_into()?))
    }
}

macro_rules! impl_from_geoarrow_type {
    ($geoarrow_type:ty, $variant:ident) => {
        impl From<$geoarrow_type> for PyGeoArrowType {
            fn from(value: $geoarrow_type) -> Self {
                Self(GeoArrowType::$variant(value))
            }
        }
    };
}

impl_from_geoarrow_type!(PointType, Point);
impl_from_geoarrow_type!(LineStringType, LineString);
impl_from_geoarrow_type!(PolygonType, Polygon);
impl_from_geoarrow_type!(MultiPointType, MultiPoint);
impl_from_geoarrow_type!(MultiLineStringType, MultiLineString);
impl_from_geoarrow_type!(MultiPolygonType, MultiPolygon);
impl_from_geoarrow_type!(GeometryType, Geometry);
impl_from_geoarrow_type!(GeometryCollectionType, GeometryCollection);
impl_from_geoarrow_type!(BoxType, Rect);
impl_from_geoarrow_type!(WkbType, Wkb);
impl_from_geoarrow_type!(WktType, Wkt);

macro_rules! impl_native_type_constructor {
    ($fn_name:ident, $geoarrow_type:ty) => {
        #[pyfunction]
        #[pyo3(signature = (dimension, coord_type, *, crs=None, edges=None))]
        pub fn $fn_name(
            dimension: PyDimension,
            coord_type: PyCoordType,
            crs: Option<PyCrs>,
            edges: Option<PyEdges>,
        ) -> PyGeoArrowType {
            let edges = edges.map(|e| e.into());
            let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
            <$geoarrow_type>::new(coord_type.into(), dimension.into(), metadata).into()
        }
    };
}

impl_native_type_constructor!(point, PointType);
impl_native_type_constructor!(linestring, LineStringType);
impl_native_type_constructor!(polygon, PolygonType);
impl_native_type_constructor!(multipoint, MultiPointType);
impl_native_type_constructor!(multilinestring, MultiLineStringType);
impl_native_type_constructor!(multipolygon, MultiPolygonType);
impl_native_type_constructor!(geometrycollection, GeometryCollectionType);

#[pyfunction]
#[pyo3(signature = (dimension, *, crs=None, edges=None))]
pub fn r#box(dimension: PyDimension, crs: Option<PyCrs>, edges: Option<PyEdges>) -> PyGeoArrowType {
    let edges = edges.map(|e| e.into());
    let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
    BoxType::new(dimension.into(), metadata).into()
}

#[pyfunction]
#[pyo3(signature = (coord_type, *, crs=None, edges=None))]
pub fn geometry(
    coord_type: PyCoordType,
    crs: Option<PyCrs>,
    edges: Option<PyEdges>,
) -> PyGeoArrowType {
    let edges = edges.map(|e| e.into());
    let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
    GeometryType::new(coord_type.into(), metadata).into()
}

#[pyfunction]
#[pyo3(signature = (*, crs=None, edges=None))]
pub fn wkb(crs: Option<PyCrs>, edges: Option<PyEdges>) -> PyGeoArrowType {
    let edges = edges.map(|e| e.into());
    let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
    WkbType::new(metadata).into()
}

#[pyfunction]
#[pyo3(signature = (*, crs=None, edges=None))]
pub fn wkt(crs: Option<PyCrs>, edges: Option<PyEdges>) -> PyGeoArrowType {
    let edges = edges.map(|e| e.into());
    let metadata = Arc::new(Metadata::new(crs.unwrap_or_default().into(), edges));
    WktType::new(metadata).into()
}
