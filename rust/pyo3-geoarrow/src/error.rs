use pyo3::CastError;
use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;

/// Error type for GeoArrow operations in Python bindings.
///
/// This enum wraps various error types that can occur during GeoArrow operations,
/// converting them to appropriate Python exceptions.
#[allow(missing_docs)]
pub enum PyGeoArrowError {
    GeoArrowError(geoarrow_schema::error::GeoArrowError),
    PyErr(PyErr),
    PyArrowError(pyo3_arrow::error::PyArrowError),
    SerdeJsonError(serde_json::Error),
    UrlParseError(url::ParseError),
}

impl From<PyGeoArrowError> for PyErr {
    fn from(error: PyGeoArrowError) -> Self {
        match error {
            PyGeoArrowError::GeoArrowError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::PyErr(err) => err,
            PyGeoArrowError::PyArrowError(err) => err.into(),
            PyGeoArrowError::SerdeJsonError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::UrlParseError(err) => PyException::new_err(err.to_string()),
        }
    }
}

impl From<geoarrow_schema::error::GeoArrowError> for PyGeoArrowError {
    fn from(other: geoarrow_schema::error::GeoArrowError) -> Self {
        Self::GeoArrowError(other)
    }
}

impl From<pyo3_arrow::error::PyArrowError> for PyGeoArrowError {
    fn from(other: pyo3_arrow::error::PyArrowError) -> Self {
        Self::PyArrowError(other)
    }
}

impl From<serde_json::Error> for PyGeoArrowError {
    fn from(other: serde_json::Error) -> Self {
        Self::SerdeJsonError(other)
    }
}

impl From<url::ParseError> for PyGeoArrowError {
    fn from(other: url::ParseError) -> Self {
        Self::UrlParseError(other)
    }
}

impl From<Bound<'_, PyTypeError>> for PyGeoArrowError {
    fn from(other: Bound<'_, PyTypeError>) -> Self {
        Self::PyErr((other).into())
    }
}

impl From<Bound<'_, PyValueError>> for PyGeoArrowError {
    fn from(other: Bound<'_, PyValueError>) -> Self {
        Self::PyErr((other).into())
    }
}

impl From<PyErr> for PyGeoArrowError {
    fn from(other: PyErr) -> Self {
        Self::PyErr(other)
    }
}

impl From<arrow_schema::ArrowError> for PyGeoArrowError {
    fn from(value: arrow_schema::ArrowError) -> Self {
        PyGeoArrowError::GeoArrowError(value.into())
    }
}

impl From<CastError<'_, '_>> for PyGeoArrowError {
    fn from(other: CastError) -> Self {
        Self::PyErr(other.into())
    }
}

/// Result type for GeoArrow operations that may fail.
pub type PyGeoArrowResult<T> = Result<T, PyGeoArrowError>;
