use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{Area, ChamberlainDuquetteArea, GeodesicArea};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

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
///     method: The method to use for area calculation. One of "Ellipsoidal", "Euclidean", or
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
pub fn area(input: AnyGeometryInput, method: AreaMethod) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_unsigned_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().unsigned_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_unsigned()?,
            };
            Python::with_gil(|py| Ok(Float64Array::from(out).into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_unsigned_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().unsigned_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_unsigned()?,
            };
            Python::with_gil(|py| Ok(ChunkedFloat64Array::from(out).into_py(py)))
        }
    }
}

/// Signed planar area of a geometry array
///
/// Args:
///     input: input geometry array or chunked geometry array
///     method: The method to use for area calculation. One of "Ellipsoidal", "Euclidean", or
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
pub fn signed_area(input: AnyGeometryInput, method: AreaMethod) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_signed_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().signed_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_signed()?,
            };
            Python::with_gil(|py| Ok(Float64Array::from(out).into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                AreaMethod::ChamberlainDuquette => {
                    arr.as_ref().chamberlain_duquette_signed_area()?
                }
                AreaMethod::Euclidean => arr.as_ref().signed_area()?,
                AreaMethod::Geodesic => arr.as_ref().geodesic_area_signed()?,
            };
            Python::with_gil(|py| Ok(ChunkedFloat64Array::from(out).into_py(py)))
        }
    }
}

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry array
            ///
            /// Args:
            ///     method: The method to use for area calculation. One of "Ellipsoidal",
            ///         "Euclidean", or "Spherical". Refer to the documentation on
            ///         [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.
            ///
            /// Returns:
            ///     Array with area values.
            #[pyo3(signature = (*, method = AreaMethod::Euclidean), text_signature = "(*, method = 'euclidean')")]
            pub fn area(&self, method: AreaMethod) -> Float64Array {
                match method {
                    AreaMethod::ChamberlainDuquette => {
                        self.0.chamberlain_duquette_unsigned_area().into()
                    }
                    AreaMethod::Euclidean => self.0.unsigned_area().into(),
                    AreaMethod::Geodesic => self.0.geodesic_area_unsigned().into(),
                }
            }

            /// Signed planar area of a geometry array
            ///
            /// Args:
            ///     method: The method to use for area calculation. One of "Ellipsoidal",
            ///         "Euclidean", or "Spherical". Refer to the documentation on
            ///         [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.
            ///
            /// Returns:
            ///     Array with area values.
            #[pyo3(signature = (*, method = AreaMethod::Euclidean), text_signature = "(*, method = 'euclidean')")]
            pub fn signed_area(&self, method: AreaMethod) -> Float64Array {
                match method {
                    AreaMethod::ChamberlainDuquette => {
                        self.0.chamberlain_duquette_signed_area().into()
                    }
                    AreaMethod::Euclidean => self.0.signed_area().into(),
                    AreaMethod::Geodesic => self.0.geodesic_area_signed().into(),
                }
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);
impl_area!(MixedGeometryArray);
impl_area!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry array
            ///
            /// Args:
            ///     method: The method to use for area calculation. One of "Ellipsoidal",
            ///         "Euclidean", or "Spherical". Refer to the documentation on
            ///         [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.
            ///
            /// Returns:
            ///     Chunked array with area values.
            #[pyo3(signature = (*, method = AreaMethod::Euclidean), text_signature = "(*, method = 'euclidean')")]
            pub fn area(&self, method: AreaMethod) -> PyGeoArrowResult<ChunkedFloat64Array> {
                match method {
                    AreaMethod::ChamberlainDuquette => {
                        Ok(self.0.chamberlain_duquette_unsigned_area()?.into())
                    }
                    AreaMethod::Euclidean => Ok(self.0.unsigned_area()?.into()),
                    AreaMethod::Geodesic => Ok(self.0.geodesic_area_unsigned()?.into()),
                }
            }

            /// Signed planar area of a geometry array
            ///
            /// Args:
            ///     method: The method to use for area calculation. One of "Ellipsoidal",
            ///         "Euclidean", or "Spherical". Refer to the documentation on
            ///         [AreaMethod][geoarrow.rust.core.enums.AreaMethod] for more information.
            ///
            /// Returns:
            ///     Chunked array with area values.
            #[pyo3(signature = (*, method = AreaMethod::Euclidean), text_signature = "(*, method = 'euclidean')")]
            pub fn signed_area(&self, method: AreaMethod) -> PyGeoArrowResult<ChunkedFloat64Array> {
                match method {
                    AreaMethod::ChamberlainDuquette => {
                        Ok(self.0.chamberlain_duquette_signed_area()?.into())
                    }
                    AreaMethod::Euclidean => Ok(self.0.signed_area()?.into()),
                    AreaMethod::Geodesic => Ok(self.0.geodesic_area_signed()?.into()),
                }
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
