use std::sync::Arc;

use crate::ffi::from_python::AnyNativeInput;
use crate::util::{return_array, return_chunked_array};
use geoarrow::algorithm::geo::{Area, ChamberlainDuquetteArea, GeodesicArea};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};
use pyo3_geoarrow::PyGeoArrowResult;

pub enum AreaMethod {
    ChamberlainDuquette,
    Euclidean,
    Geodesic,
}

impl<'a> FromPyObject<'a> for AreaMethod {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "ellipsoidal" => Ok(Self::Geodesic),
            "euclidean" => Ok(Self::Euclidean),
            "spherical" => Ok(Self::ChamberlainDuquette),
            _ => Err(PyValueError::new_err("Unexpected area method")),
        }
    }
}

#[pyfunction]
#[pyo3(
    signature = (input, *, method = AreaMethod::Euclidean),
    text_signature = "(input, *, method = 'euclidean')")
]
pub fn area(py: Python, input: AnyNativeInput, method: AreaMethod) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_unsigned_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().unsigned_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_unsigned()?,
            };
            return_array(py, PyArray::from_array_ref(Arc::new(out)))
        }
        AnyNativeInput::Chunked(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_unsigned_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().unsigned_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_unsigned()?,
            };
            return_chunked_array(py, PyChunkedArray::from_array_refs(out.chunk_refs())?)
        }
    }
}

#[pyfunction]
#[pyo3(
    signature = (input, *, method = AreaMethod::Euclidean),
    text_signature = "(input, *, method = 'euclidean')")
]
pub fn signed_area(
    py: Python,
    input: AnyNativeInput,
    method: AreaMethod,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_signed_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().signed_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_signed()?,
            };
            return_array(py, PyArray::from_array_ref(Arc::new(out)))
        }
        AnyNativeInput::Chunked(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_signed_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().signed_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_signed()?,
            };
            return_chunked_array(py, PyChunkedArray::from_array_refs(out.chunk_refs())?)
        }
    }
}
