use geoarrow_schema::crs::CrsTransform;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{Crs, CrsType};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyTuple;
use pyo3::{BoundObject, intern};
use serde_json::Value;

use crate::PyGeoArrowError;
use crate::error::PyGeoArrowResult;

/// Python wrapper for a coordinate reference system (CRS).
///
/// This type integrates with the `pyproj` Python library, allowing CRS definitions to be
/// passed between Rust and Python. It can accept any input that `pyproj.CRS.from_user_input`
/// accepts, including EPSG codes, WKT strings, PROJ strings, and `pyproj.CRS` objects.
#[derive(Clone, Debug, Default)]
// TODO: should this be under an Arc?
pub struct PyCrs(Crs);

impl<'py> FromPyObject<'_, 'py> for PyCrs {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let ob = ob.into_bound();
        let py = ob.py();
        let pyproj = py.import(intern!(py, "pyproj"))?;
        let crs_class = pyproj.getattr(intern!(py, "CRS"))?;

        let mut ob = ob.clone();

        // If the input is not a pyproj.CRS, call pyproj.CRS.from_user_input on it
        if !ob.is_instance(&crs_class)? {
            let args = PyTuple::new(py, vec![ob])?;
            ob = crs_class.call_method1(intern!(py, "from_user_input"), args)?;
        }

        let projjson_string = ob
            .call_method0(intern!(py, "to_json"))?
            .extract::<PyBackedStr>()?;
        let projjson_value = serde_json::from_str(&projjson_string)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(Crs::from_projjson(projjson_value)))
    }
}

impl PyCrs {
    /// Create a [`PyCrs`] from a PROJJSON value.
    pub fn from_projjson(value: Value) -> Self {
        Self(Crs::from_projjson(value))
    }

    /// Export the embedded CRS to a `pyproj.CRS` or None
    pub fn to_pyproj(&self, py: Python) -> PyGeoArrowResult<Py<PyAny>> {
        let pyproj = py.import(intern!(py, "pyproj"))?;
        let crs_class = pyproj.getattr(intern!(py, "CRS"))?;

        let crs_obj = match self.0.crs_type() {
            Some(CrsType::Projjson) => {
                let args = PyTuple::new(
                    py,
                    vec![serde_json::to_string(
                        &self.0.crs_value().as_ref().unwrap(),
                    )?],
                )?;
                crs_class.call_method1(intern!(py, "from_json"), args)?
            }
            Some(CrsType::AuthorityCode) => match self.0.crs_value().as_ref().unwrap() {
                Value::String(value) => {
                    let (authority, code) =
                        value.split_once(':').expect("expected : in authority code");
                    let args = PyTuple::new(py, vec![authority, code])?;
                    crs_class.call_method1(intern!(py, "from_authority"), args)?
                }
                _ => return Err(PyValueError::new_err(
                    "Invalid GeoArrow metadata: Expected string CRS value with CRS type Authority Code",
                ).into()),
            },
            Some(CrsType::Wkt2_2019) => match self.0.crs_value().as_ref().unwrap() {
                Value::String(value) => {
                    let args = PyTuple::new(py, vec![value])?;
                    crs_class.call_method1(intern!(py, "from_wkt"), args)?
                }
                _ => return Err(PyValueError::new_err(
                    "Invalid GeoArrow metadata: Expected string CRS value with CRS type WKT2:2019",
                ).into()),
            },
            _ => match self.0.crs_value().as_ref() {
                None => py.None().into_bound(py),
                Some(Value::Object(_)) => {
                    let args = PyTuple::new(
                        py,
                        vec![serde_json::to_string(
                            &self.0.crs_value().as_ref().unwrap(),
                        )?],
                    )?;
                    crs_class.call_method1(intern!(py, "from_json"), args)?
                }
                Some(Value::String(value)) => {
                    let args = PyTuple::new(py, vec![value])?;
                    crs_class.call_method1(intern!(py, "from_user_input"), args)?
                }
                _ => return Err(PyValueError::new_err(
                    "Invalid GeoArrow metadata: Expected missing CRS or string or object value with unknown CRS type",
                ).into()),
            },
        };
        Ok(crs_obj.into())
    }

    /// Convert the CRS to a PROJJSON value.
    pub fn to_projjson(&self, py: Python) -> PyResult<Option<Value>> {
        let pyproj_crs = self.to_pyproj(py)?;
        if pyproj_crs.is_none(py) {
            Ok(None)
        } else {
            let projjson_str = pyproj_crs
                .call_method0(py, intern!(py, "to_json"))?
                .extract::<PyBackedStr>(py)?;

            let projjson_value: Value = serde_json::from_str(&projjson_str)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;
            Ok(Some(projjson_value))
        }
    }

    /// Convert the CRS to a WKT string.
    pub fn to_wkt(&self, py: Python) -> PyResult<Option<String>> {
        let pyproj_crs = self.to_pyproj(py)?;
        if pyproj_crs.is_none(py) {
            Ok(None)
        } else {
            let args = PyTuple::new(py, vec![intern!(py, "WKT2_2019")])?;
            let wkt_str = pyproj_crs
                .call_method1(py, intern!(py, "to_wkt"), args)?
                .extract::<String>(py)?;

            Ok(Some(wkt_str))
        }
    }
}

impl From<Crs> for PyCrs {
    fn from(value: Crs) -> Self {
        Self(value)
    }
}

impl From<PyCrs> for Crs {
    fn from(value: PyCrs) -> Self {
        value.0
    }
}

impl<'py> IntoPyObject<'py> for PyCrs {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyGeoArrowError;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.to_pyproj(py)?.bind(py).clone())
    }
}

/// An implementation of [`CrsTransform`] using pyproj.
///
/// This type enables CRS transformations by delegating to the pyproj Python library,
/// allowing conversion between different CRS representations (PROJJSON, WKT, etc.).
#[derive(Debug)]
pub struct PyprojCRSTransform {}

impl PyprojCRSTransform {
    /// Create a new [`PyprojCRSTransform`].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for PyprojCRSTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl CrsTransform for PyprojCRSTransform {
    fn _convert_to_projjson(&self, crs: &Crs) -> GeoArrowResult<Option<Value>> {
        let crs = PyCrs::from(crs.clone());
        let projjson = Python::attach(|py| crs.to_projjson(py))
            .map_err(|err| GeoArrowError::Crs(err.to_string()))?;
        Ok(projjson)
    }

    fn _convert_to_wkt(&self, crs: &Crs) -> GeoArrowResult<Option<String>> {
        let crs = PyCrs::from(crs.clone());
        let wkt = Python::attach(|py| crs.to_wkt(py))
            .map_err(|err| GeoArrowError::Crs(err.to_string()))?;
        Ok(wkt)
    }
}
