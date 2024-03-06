use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::AffineOps;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub struct AffineTransform(geo::AffineTransform);

impl<'a> FromPyObject<'a> for AffineTransform {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if let Ok(transform) = ob.extract::<[f64; 6]>() {
            Ok(Self(transform.into()))
        } else if let Ok(transform) = ob.extract::<[f64; 9]>() {
            if transform[6] != 0. || transform[7] != 0. || transform[8] != 1. {
                return Err(PyValueError::new_err(
                    "If 9 values passed, last three must be `0, 0, 1",
                ));
            }

            let transform: [f64; 6] = transform[..6].try_into().unwrap();
            Ok(Self(transform.into()))
        } else {
            Err(PyValueError::new_err("Expected tuple with 6 or 9 elements"))
        }
    }
}

/// Apply an affine transformation to geometries
///
/// This function is not yet vectorized
#[pyfunction]
pub fn affine_transform(
    input: AnyGeometryInput,
    transform: AffineTransform,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().affine_transform(&transform.0)?;
            Python::with_gil(|py| geometry_array_to_pyobject(py, out))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().affine_transform(&transform.0)?;
            Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, out))
        }
    }
}

macro_rules! impl_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            pub fn affine_transform(&self, transform: AffineTransform) -> Self {
                self.0.affine_transform(&transform.0).into()
            }
        }
    };
}

impl_array!(PointArray);
impl_array!(LineStringArray);
impl_array!(PolygonArray);
impl_array!(MultiPointArray);
impl_array!(MultiLineStringArray);
impl_array!(MultiPolygonArray);
impl_array!(MixedGeometryArray);
impl_array!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            pub fn affine_transform(&self, transform: AffineTransform) -> Self {
                self.0.affine_transform(&transform.0).into()
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
