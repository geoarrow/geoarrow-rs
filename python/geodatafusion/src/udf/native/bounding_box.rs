use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use geodatafusion::udf::native::bounding_box::{
    Box2D, Box3D, MakeBox2D, MakeBox3D, XMax, XMin, YMax, YMin, ZMax, ZMin,
};
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

use crate::constants::DATAFUSION_CAPSULE_NAME;

#[pyclass(module = "geodatafusion", name = "Box2D", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyBox2D(Arc<Box2D>);

#[pymethods]
impl PyBox2D {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Box2D::new()))
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

#[pyclass(module = "geodatafusion", name = "Box3D", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyBox3D(Arc<Box3D>);

#[pymethods]
impl PyBox3D {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Box3D::new()))
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

#[pyclass(module = "geodatafusion", name = "XMin", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyXMin(Arc<XMin>);

#[pymethods]
impl PyXMin {
    #[new]
    fn new() -> Self {
        Self(Arc::new(XMin::new()))
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

#[pyclass(module = "geodatafusion", name = "XMax", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyXMax(Arc<XMax>);

#[pymethods]
impl PyXMax {
    #[new]
    fn new() -> Self {
        Self(Arc::new(XMax::new()))
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

#[pyclass(module = "geodatafusion", name = "YMin", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyYMin(Arc<YMin>);

#[pymethods]
impl PyYMin {
    #[new]
    fn new() -> Self {
        Self(Arc::new(YMin::new()))
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

#[pyclass(module = "geodatafusion", name = "YMax", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyYMax(Arc<YMax>);

#[pymethods]
impl PyYMax {
    #[new]
    fn new() -> Self {
        Self(Arc::new(YMax::new()))
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

#[pyclass(module = "geodatafusion", name = "ZMin", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyZMin(Arc<ZMin>);

#[pymethods]
impl PyZMin {
    #[new]
    fn new() -> Self {
        Self(Arc::new(ZMin::new()))
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

#[pyclass(module = "geodatafusion", name = "ZMax", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyZMax(Arc<ZMax>);

#[pymethods]
impl PyZMax {
    #[new]
    fn new() -> Self {
        Self(Arc::new(ZMax::new()))
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

#[pyclass(module = "geodatafusion", name = "MakeBox2D", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyMakeBox2D(Arc<MakeBox2D>);

#[pymethods]
impl PyMakeBox2D {
    #[new]
    fn new() -> Self {
        Self(Arc::new(MakeBox2D::new()))
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

#[pyclass(module = "geodatafusion", name = "MakeBox3D", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyMakeBox3D(Arc<MakeBox3D>);

#[pymethods]
impl PyMakeBox3D {
    #[new]
    fn new() -> Self {
        Self(Arc::new(MakeBox3D::new()))
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
