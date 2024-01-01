use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;

pub enum PyGeoArrowError {
    GeoArrowError(geoarrow::error::GeoArrowError),
    PyErr(PyErr),
}

impl From<PyGeoArrowError> for PyErr {
    fn from(error: PyGeoArrowError) -> Self {
        match error {
            PyGeoArrowError::GeoArrowError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::PyErr(err) => err,
        }
    }
}

impl From<geoarrow::error::GeoArrowError> for PyGeoArrowError {
    fn from(other: geoarrow::error::GeoArrowError) -> Self {
        Self::GeoArrowError(other)
    }
}

impl From<PyTypeError> for PyGeoArrowError {
    fn from(other: PyTypeError) -> Self {
        Self::PyErr((&other).into())
    }
}

impl From<PyValueError> for PyGeoArrowError {
    fn from(other: PyValueError) -> Self {
        Self::PyErr((&other).into())
    }
}

impl From<PyErr> for PyGeoArrowError {
    fn from(other: PyErr) -> Self {
        Self::PyErr(other)
    }
}

impl From<arrow::error::ArrowError> for PyGeoArrowError {
    fn from(value: arrow::error::ArrowError) -> Self {
        PyGeoArrowError::GeoArrowError(value.into())
    }
}

pub type PyGeoArrowResult<T> = Result<T, PyGeoArrowError>;
