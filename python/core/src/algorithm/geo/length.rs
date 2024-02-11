use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{EuclideanLength, GeodesicLength, HaversineLength, VincentyLength};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub enum LengthMethod {
    Euclidean,
    Geodesic,
    Haversine,
    Vincenty,
}

impl<'a> FromPyObject<'a> for LengthMethod {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
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

/// Calculation of the length of a Line
///
/// Args:
///     input: input geometry array or chunked geometry array
///
/// Other args:
///      method: The method to use for length calculation. One of "Ellipsoidal", "Euclidean",
///         "Haversine", or "Vincenty". Refer to the documentation on
///         [LengthMethod][geoarrow.rust.core.enums.LengthMethod] for more information.
///
/// Returns:
///     Array or chunked array with length values.
#[pyfunction]
#[pyo3(
    signature = (input, *, method = LengthMethod::Euclidean),
    text_signature = "(input, *, method = 'euclidean')")
]
pub fn length(input: AnyGeometryInput, method: LengthMethod) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                LengthMethod::Euclidean => arr.as_ref().euclidean_length()?,
                LengthMethod::Geodesic => arr.as_ref().geodesic_length()?,
                LengthMethod::Haversine => arr.as_ref().haversine_length()?,
                LengthMethod::Vincenty => arr.as_ref().vincenty_length()?,
            };
            Python::with_gil(|py| Ok(Float64Array::from(out).into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                LengthMethod::Euclidean => arr.as_ref().euclidean_length()?,
                LengthMethod::Geodesic => arr.as_ref().geodesic_length()?,
                LengthMethod::Haversine => arr.as_ref().haversine_length()?,
                LengthMethod::Vincenty => arr.as_ref().vincenty_length()?,
            };
            Python::with_gil(|py| Ok(ChunkedFloat64Array::from(out).into_py(py)))
        }
    }
}
macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the length of a Line
            ///
            /// Other args:
            ///      method: The method to use for length calculation. One of "Ellipsoidal", "Euclidean",
            ///         "Haversine", or "Vincenty". Refer to the documentation on
            ///         [LengthMethod][geoarrow.rust.core.enums.LengthMethod] for more information.
            ///
            /// Returns:
            ///     Array with length values.
            #[pyo3(signature = (*, method = LengthMethod::Euclidean), text_signature = "(*, method = 'euclidean')")]
            pub fn length(&self, method: LengthMethod) -> PyGeoArrowResult<Float64Array> {
                match method {
                    LengthMethod::Euclidean => Ok(self.0.euclidean_length().into()),
                    LengthMethod::Geodesic => Ok(self.0.geodesic_length().into()),
                    LengthMethod::Haversine => Ok(self.0.haversine_length().into()),
                    LengthMethod::Vincenty => Ok(self.0.vincenty_length()?.into()),
                }
            }
        }
    };
}

impl_euclidean_length!(PointArray);
impl_euclidean_length!(MultiPointArray);
impl_euclidean_length!(LineStringArray);
impl_euclidean_length!(MultiLineStringArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the length of a Line
            ///
            /// Other args:
            ///      method: The method to use for length calculation. One of "Ellipsoidal", "Euclidean",
            ///         "Haversine", or "Vincenty". Refer to the documentation on
            ///         [LengthMethod][geoarrow.rust.core.enums.LengthMethod] for more information.
            ///
            /// Returns:
            ///     Chunked array with length values.
            #[pyo3(signature = (*, method = LengthMethod::Euclidean), text_signature = "(*, method = 'euclidean')")]
            pub fn length(&self, method: LengthMethod) -> PyGeoArrowResult<ChunkedFloat64Array> {
                match method {
                    LengthMethod::Euclidean => Ok(self.0.euclidean_length()?.into()),
                    LengthMethod::Geodesic => Ok(self.0.geodesic_length()?.into()),
                    LengthMethod::Haversine => Ok(self.0.haversine_length()?.into()),
                    LengthMethod::Vincenty => Ok(self.0.vincenty_length()?.into()),
                }
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedMultiLineStringArray);
