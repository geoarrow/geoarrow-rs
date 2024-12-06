use geoarrow::array::metadata::{ArrayMetadata, CRSType};
use geoarrow::error::GeoArrowError;
use geoarrow::io::crs::CRSTransform;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyTuple;
use serde_json::Value;

use crate::error::PyGeoArrowResult;

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
pub struct CRS(ArrayMetadata);

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
        let projjson_value = serde_json::from_str(&projjson_string)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(ArrayMetadata {
            crs: Some(projjson_value),
            crs_type: Some(CRSType::Projjson),
            ..Default::default()
        }))
    }
}

impl CRS {
    pub fn from_projjson(value: Value) -> Self {
        Self(ArrayMetadata::from_projjson(value))
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> ArrayMetadata {
        self.0
    }

    /// Export the embedded CRS to a `pyproj.CRS` or None
    pub fn to_pyproj(&self, py: Python) -> PyGeoArrowResult<PyObject> {
        let pyproj = py.import(intern!(py, "pyproj"))?;
        let crs_class = pyproj.getattr(intern!(py, "CRS"))?;

        let crs_obj = match self.0.crs_type {
            Some(CRSType::Projjson) => {
                let args = PyTuple::new(
                    py,
                    vec![serde_json::to_string(&self.0.crs.as_ref().unwrap())?],
                )?;
                crs_class.call_method1(intern!(py, "from_json"), args)?
            }
            Some(CRSType::AuthorityCode) => match self.0.crs.as_ref().unwrap() {
                Value::String(value) => {
                    let (authority, code) =
                        value.split_once(':').expect("expected : in authority code");
                    let args = PyTuple::new(py, vec![authority, code])?;
                    crs_class.call_method1(intern!(py, "from_json"), args)?
                }
                _ => panic!("Expected string value"),
            },
            Some(CRSType::Wkt2_2019) => match self.0.crs.as_ref().unwrap() {
                Value::String(value) => {
                    let args = PyTuple::new(py, vec![value])?;
                    crs_class.call_method1(intern!(py, "from_wkt"), args)?
                }
                _ => panic!("Expected string value"),
            },
            _ => match self.0.crs.as_ref() {
                None => py.None().into_bound(py),
                Some(Value::String(value)) => {
                    let args = PyTuple::new(py, vec![value])?;
                    crs_class.call_method1(intern!(py, "from_user_input"), args)?
                }
                _ => panic!("Expected missing CRS or string value"),
            },
        };
        Ok(crs_obj.into())
    }

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

/// An implementation of [CRSTransform] using pyproj.
#[derive(Debug)]
pub struct PyprojCRSTransform {}

impl PyprojCRSTransform {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for PyprojCRSTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl CRSTransform for PyprojCRSTransform {
    fn _convert_to_projjson(
        &self,
        meta: &geoarrow::array::metadata::ArrayMetadata,
    ) -> geoarrow::error::Result<Option<Value>> {
        let crs = CRS(meta.clone());
        let projjson = Python::with_gil(|py| crs.to_projjson(py))
            .map_err(|err| GeoArrowError::General(err.to_string()))?;
        Ok(projjson)
    }

    fn _convert_to_wkt(&self, meta: &ArrayMetadata) -> geoarrow::error::Result<Option<String>> {
        let crs = CRS(meta.clone());
        let wkt = Python::with_gil(|py| crs.to_wkt(py))
            .map_err(|err| GeoArrowError::General(err.to_string()))?;
        Ok(wkt)
    }
}
