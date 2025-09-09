use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use geodatafusion::udf::native::io::{AsBinary, AsText, GeomFromText, GeomFromWKB};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use pyo3_geoarrow::PyCoordType;
use std::sync::Arc;

use crate::constants::DATAFUSION_CAPSULE_NAME;

#[pyclass(module = "geodatafusion", name = "AsBinary", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyAsBinary(Arc<AsBinary>);

#[pymethods]
impl PyAsBinary {
    #[new]
    fn new() -> Self {
        Self(Arc::new(AsBinary::new()))
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::new_from_shared_impl(self.0.clone()));
        PyCapsule::new(
            py,
            FFI_ScalarUDF::from(udf),
            Some(DATAFUSION_CAPSULE_NAME.into()),
        )
    }
}

#[pyclass(module = "geodatafusion", name = "GeomFromWkb", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyGeomFromWKB(Arc<GeomFromWKB>);

#[pymethods]
impl PyGeomFromWKB {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(GeomFromWKB::new(coord_type)))
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::new_from_shared_impl(self.0.clone()));
        PyCapsule::new(
            py,
            FFI_ScalarUDF::from(udf),
            Some(DATAFUSION_CAPSULE_NAME.into()),
        )
    }
}

#[pyclass(module = "geodatafusion", name = "AsText", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyAsText(Arc<AsText>);

#[pymethods]
impl PyAsText {
    #[new]
    fn new() -> Self {
        Self(Arc::new(AsText::new()))
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::new_from_shared_impl(self.0.clone()));
        PyCapsule::new(
            py,
            FFI_ScalarUDF::from(udf),
            Some(DATAFUSION_CAPSULE_NAME.into()),
        )
    }
}

#[pyclass(module = "geodatafusion", name = "GeomFromText", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyGeomFromText(Arc<GeomFromText>);

#[pymethods]
impl PyGeomFromText {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(GeomFromText::new(coord_type)))
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::new_from_shared_impl(self.0.clone()));
        PyCapsule::new(
            py,
            FFI_ScalarUDF::from(udf),
            Some(DATAFUSION_CAPSULE_NAME.into()),
        )
    }
}
