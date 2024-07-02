use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{Area, ChamberlainDuquetteArea, GeodesicArea};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

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

/// Determine the area of an array of geometries
///
/// Args:
///     input: input geometry array or chunked geometry array
///
/// Other args:
///      method: The method to use for area calculation. One of "Ellipsoidal", "Euclidean", or
///         "Spherical". Refer to the documentation on
///         [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.
///
/// Returns:
///     Array or chunked array with area values.
#[pyfunction]
#[pyo3(
    signature = (input, *, method = AreaMethod::Euclidean),
    text_signature = "(input, *, method = 'euclidean')")
]
pub fn area(py: Python, input: AnyGeometryInput, method: AreaMethod) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_unsigned_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().unsigned_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_unsigned()?,
            };
            Ok(PyArray::from_array(out).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_unsigned_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().unsigned_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_unsigned()?,
            };
            Ok(PyChunkedArray::from_arrays(out.chunks())?.to_arro3(py)?)
        }
    }
}

/// Signed area of a geometry array
///
/// Args:
///     input: input geometry array or chunked geometry array
///
/// Other args:
///      method: The method to use for area calculation. One of "Ellipsoidal", "Euclidean", or
///         "Spherical". Refer to the documentation on
///         [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.
///
/// Returns:
///     Array or chunked array with area values.
#[pyfunction]
#[pyo3(
    signature = (input, *, method = AreaMethod::Euclidean),
    text_signature = "(input, *, method = 'euclidean')")
]
pub fn signed_area(
    py: Python,
    input: AnyGeometryInput,
    method: AreaMethod,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_signed_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().signed_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_signed()?,
            };
            Ok(PyArray::from_array(out).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_signed_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().signed_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_signed()?,
            };
            Ok(PyChunkedArray::from_arrays(out.chunks())?.to_arro3(py)?)
        }
    }
}
