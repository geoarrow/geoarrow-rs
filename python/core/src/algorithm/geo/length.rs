use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{EuclideanLength, GeodesicLength, HaversineLength, VincentyLength};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

pub enum LengthMethod {
    Euclidean,
    Geodesic,
    Haversine,
    Vincenty,
}

impl<'a> FromPyObject<'a> for LengthMethod {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "ellipsoidal" => Ok(Self::Geodesic),
            "euclidean" => Ok(Self::Euclidean),
            "haversine" => Ok(Self::Haversine),
            "vincenty" => Ok(Self::Vincenty),
            _ => Err(PyValueError::new_err("Unexpected length method")),
        }
    }
}

#[pyfunction]
#[pyo3(
    signature = (input, *, method = LengthMethod::Euclidean),
    text_signature = "(input, *, method = 'euclidean')")
]
pub fn length(
    py: Python,
    input: AnyGeometryInput,
    method: LengthMethod,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                LengthMethod::Euclidean => arr.as_ref().euclidean_length()?,
                LengthMethod::Geodesic => arr.as_ref().geodesic_length()?,
                LengthMethod::Haversine => arr.as_ref().haversine_length()?,
                LengthMethod::Vincenty => arr.as_ref().vincenty_length()?,
            };
            Ok(PyArray::from_array(out).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                LengthMethod::Euclidean => arr.as_ref().euclidean_length()?,
                LengthMethod::Geodesic => arr.as_ref().geodesic_length()?,
                LengthMethod::Haversine => arr.as_ref().haversine_length()?,
                LengthMethod::Vincenty => arr.as_ref().vincenty_length()?,
            };
            Ok(PyChunkedArray::from_arrays(out.chunks())?.to_arro3(py)?)
        }
    }
}
