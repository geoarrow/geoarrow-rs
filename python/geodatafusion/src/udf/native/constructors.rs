use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use geodatafusion::udf::native::constructors::{
    MakePoint, MakePointM, Point, PointM, PointZ, PointZM,
};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use pyo3_geoarrow::PyCoordType;
use std::sync::Arc;

use crate::constants::DATAFUSION_CAPSULE_NAME;

#[pyclass(module = "geodatafusion", name = "Point", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyPoint(Arc<Point>);

#[pymethods]
impl PyPoint {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(Point::new(coord_type)))
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

#[pyclass(module = "geodatafusion", name = "PointZ", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyPointZ(Arc<PointZ>);

#[pymethods]
impl PyPointZ {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(PointZ::new(coord_type)))
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

#[pyclass(module = "geodatafusion", name = "PointM", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyPointM(Arc<PointM>);

#[pymethods]
impl PyPointM {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(PointM::new(coord_type)))
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

#[pyclass(module = "geodatafusion", name = "PointZM", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyPointZM(Arc<PointZM>);

#[pymethods]
impl PyPointZM {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(PointZM::new(coord_type)))
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

#[pyclass(module = "geodatafusion", name = "MakePoint", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyMakePoint(Arc<MakePoint>);

#[pymethods]
impl PyMakePoint {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(MakePoint::new(coord_type)))
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

#[pyclass(module = "geodatafusion", name = "MakePointM", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyMakePointM(Arc<MakePointM>);

#[pymethods]
impl PyMakePointM {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(MakePointM::new(coord_type)))
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
