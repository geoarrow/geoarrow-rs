use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use geodatafusion::udf::native::accessors::{CoordDim, M, NDims, X, Y, Z};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

use crate::constants::DATAFUSION_CAPSULE_NAME;

#[pyclass(module = "geodatafusion", name = "CoordDim", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyCoordDim(Arc<CoordDim>);

#[pymethods]
impl PyCoordDim {
    #[new]
    fn new() -> Self {
        Self(Arc::new(CoordDim::new()))
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

#[pyclass(module = "geodatafusion", name = "NDims", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyNDims(Arc<NDims>);

#[pymethods]
impl PyNDims {
    #[new]
    fn new() -> Self {
        Self(Arc::new(NDims::new()))
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

#[pyclass(module = "geodatafusion", name = "X", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyX(Arc<X>);

#[pymethods]
impl PyX {
    #[new]
    fn new() -> Self {
        Self(Arc::new(X::new()))
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

#[pyclass(module = "geodatafusion", name = "Y", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyY(Arc<Y>);

#[pymethods]
impl PyY {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Y::new()))
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

#[pyclass(module = "geodatafusion", name = "Z", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyZ(Arc<Z>);

#[pymethods]
impl PyZ {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Z::new()))
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

#[pyclass(module = "geodatafusion", name = "M", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyM(Arc<M>);

#[pymethods]
impl PyM {
    #[new]
    fn new() -> Self {
        Self(Arc::new(M::new()))
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
