use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use serde_json::Value;

use crate::error::PyGeoArrowResult;

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
pub struct CRS(Value);

impl<'py> FromPyObject<'py> for CRS {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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
            .extract::<String>()?;
        let value = serde_json::from_str(&projjson_string)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(value))
    }
}

impl CRS {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Value {
        self.0
    }

    pub fn to_pyproj(&self, py: Python) -> PyGeoArrowResult<PyObject> {
        let pyproj = py.import(intern!(py, "pyproj"))?;
        let crs_class = pyproj.getattr(intern!(py, "CRS"))?;

        let args = PyTuple::new(py, vec![serde_json::to_string(&self.0)?])?;
        let crs_obj = crs_class.call_method1(intern!(py, "from_json"), args)?;
        Ok(crs_obj.into())
    }
}
