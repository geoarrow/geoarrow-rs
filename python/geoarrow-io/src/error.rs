use pyo3::exceptions::{PyException, PyIOError, PyTypeError, PyValueError};
use pyo3::prelude::*;

pub enum PyGeoArrowError {
    ParquetError(parquet::errors::ParquetError),
    GeoArrowError(geoarrow_schema::error::GeoArrowError),
    IOError(std::io::Error),
    PyArrowError(pyo3_arrow::error::PyArrowError),
    PyErr(PyErr),
    #[cfg(feature = "async")]
    ObjectStoreError(object_store::Error),
    #[cfg(feature = "async")]
    ObjectStorePathError(object_store::path::Error),
    SerdeJsonError(serde_json::Error),
    UrlParseError(url::ParseError),
}

impl From<PyGeoArrowError> for PyErr {
    fn from(error: PyGeoArrowError) -> Self {
        match error {
            PyGeoArrowError::ParquetError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::GeoArrowError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::IOError(err) => PyIOError::new_err(err.to_string()),
            PyGeoArrowError::PyErr(err) => err,
            PyGeoArrowError::PyArrowError(err) => err.into(),
            #[cfg(feature = "async")]
            PyGeoArrowError::ObjectStoreError(err) => PyException::new_err(err.to_string()),
            #[cfg(feature = "async")]
            PyGeoArrowError::ObjectStorePathError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::SerdeJsonError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::UrlParseError(err) => PyException::new_err(err.to_string()),
        }
    }
}

impl From<parquet::errors::ParquetError> for PyGeoArrowError {
    fn from(value: parquet::errors::ParquetError) -> Self {
        Self::ParquetError(value)
    }
}

impl From<geoarrow_schema::error::GeoArrowError> for PyGeoArrowError {
    fn from(other: geoarrow_schema::error::GeoArrowError) -> Self {
        Self::GeoArrowError(other)
    }
}

impl From<std::io::Error> for PyGeoArrowError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<pyo3_arrow::error::PyArrowError> for PyGeoArrowError {
    fn from(other: pyo3_arrow::error::PyArrowError) -> Self {
        Self::PyArrowError(other)
    }
}

#[cfg(feature = "async")]
impl From<object_store::Error> for PyGeoArrowError {
    fn from(other: object_store::Error) -> Self {
        Self::ObjectStoreError(other)
    }
}

#[cfg(feature = "async")]
impl From<object_store::path::Error> for PyGeoArrowError {
    fn from(other: object_store::path::Error) -> Self {
        Self::ObjectStorePathError(other)
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
        Self::PyErr(other.into())
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

impl From<arrow::error::ArrowError> for PyGeoArrowError {
    fn from(value: arrow::error::ArrowError) -> Self {
        PyGeoArrowError::GeoArrowError(value.into())
    }
}

pub type PyGeoArrowResult<T> = Result<T, PyGeoArrowError>;
