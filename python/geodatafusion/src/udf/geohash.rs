use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use geodatafusion::udf::geohash::{Box2DFromGeoHash, GeoHash, PointFromGeoHash};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use pyo3_geoarrow::PyCoordType;
use std::sync::Arc;

use crate::constants::DATAFUSION_CAPSULE_NAME;

#[pyclass(module = "geodatafusion", name = "Box2DFromGeoHash", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyBox2DFromGeoHash(Arc<Box2DFromGeoHash>);

#[pymethods]
impl PyBox2DFromGeoHash {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Box2DFromGeoHash::new()))
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

#[pyclass(module = "geodatafusion", name = "GeoHash", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyGeoHash(Arc<GeoHash>);

#[pymethods]
impl PyGeoHash {
    #[new]
    fn new() -> Self {
        Self(Arc::new(GeoHash::new()))
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

#[pyclass(module = "geodatafusion", name = "PointFromGeoHash", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct PyPointFromGeoHash(Arc<PointFromGeoHash>);

#[pymethods]
impl PyPointFromGeoHash {
    #[new]
    #[pyo3(signature = (*, coord_type=None))]
    fn new(coord_type: Option<PyCoordType>) -> Self {
        let coord_type = coord_type.map(|c| c.into()).unwrap_or_default();
        Self(Arc::new(PointFromGeoHash::new(coord_type)))
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

#[pymodule]
pub(crate) fn geohash(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyGeoHash>()?;
    m.add_class::<PyBox2DFromGeoHash>()?;
    m.add_class::<PyPointFromGeoHash>()?;

    Ok(())
}
