use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;

pub enum PyGeoArrowError {
    GeoArrowError(geoarrow::error::GeoArrowError),
    PyErr(PyErr),
    PyArrowError(pyo3_arrow::error::PyArrowError),
<<<<<<< HEAD
=======
    PythonizeError(pythonize::PythonizeError),
    #[cfg(feature = "async")]
    ObjectStoreError(object_store::Error),
    #[cfg(feature = "async")]
    ObjectStorePathError(object_store::path::Error),
>>>>>>> main
    SerdeJsonError(serde_json::Error),
    UrlParseError(url::ParseError),
}

impl From<PyGeoArrowError> for PyErr {
    fn from(error: PyGeoArrowError) -> Self {
        match error {
            PyGeoArrowError::GeoArrowError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::PyErr(err) => err,
            PyGeoArrowError::PyArrowError(err) => err.into(),
<<<<<<< HEAD
=======
            PyGeoArrowError::PythonizeError(err) => PyException::new_err(err.to_string()),
            #[cfg(feature = "async")]
            PyGeoArrowError::ObjectStoreError(err) => PyException::new_err(err.to_string()),
            #[cfg(feature = "async")]
            PyGeoArrowError::ObjectStorePathError(err) => PyException::new_err(err.to_string()),
>>>>>>> main
            PyGeoArrowError::SerdeJsonError(err) => PyException::new_err(err.to_string()),
            PyGeoArrowError::UrlParseError(err) => PyException::new_err(err.to_string()),
        }
    }
}

impl From<geoarrow::error::GeoArrowError> for PyGeoArrowError {
    fn from(other: geoarrow::error::GeoArrowError) -> Self {
        Self::GeoArrowError(other)
    }
}

impl From<pyo3_arrow::error::PyArrowError> for PyGeoArrowError {
    fn from(other: pyo3_arrow::error::PyArrowError) -> Self {
        Self::PyArrowError(other)
    }
}

<<<<<<< HEAD
=======
impl From<pythonize::PythonizeError> for PyGeoArrowError {
    fn from(other: pythonize::PythonizeError) -> Self {
        Self::PythonizeError(other)
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

>>>>>>> main
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
