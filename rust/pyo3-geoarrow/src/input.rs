use std::sync::Arc;

use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::ffi::{ArrayIterator, ArrayReader};

use crate::{PyGeoArray, PyGeoArrayReader, PyGeoArrowResult, PyGeoChunkedArray};

/// An enum over [PyGeoArray] and [PyGeoArrayReader], used when a function accepts either
/// Arrow object as input.
pub enum AnyGeoArray {
    /// A single Array, held in a [PyGeoArray].
    Array(PyGeoArray),
    /// A stream of possibly multiple Arrays, held in a [PyGeoArrayReader].
    Stream(PyGeoArrayReader),
}

impl AnyGeoArray {
    /// Consume this and convert it into a [PyGeoChunkedArray].
    ///
    /// All arrays from the stream will be materialized in memory.
    pub fn into_chunked_array(self) -> PyGeoArrowResult<PyGeoChunkedArray> {
        let data_type = self.data_type();
        let reader = self.into_reader()?;
        let field = reader.field();
        let chunks = reader
            .map(|array| from_arrow_array(array?.as_ref(), &field))
            .collect::<Result<_, GeoArrowError>>()?;

        Ok(PyGeoChunkedArray::new(chunks, data_type))
    }

    pub fn into_reader(self) -> PyResult<Box<dyn ArrayReader + Send>> {
        match self {
            Self::Array(array) => {
                let geo_array = array.into_inner();
                let field = Arc::new(geo_array.data_type().to_field("", true));
                let array = geo_array.to_array_ref();
                Ok(Box::new(ArrayIterator::new(vec![Ok(array)], field)))
            }
            Self::Stream(stream) => stream.into_inner().0.into_reader(),
        }
    }

    pub fn data_type(&self) -> GeoArrowType {
        match self {
            Self::Array(array) => array.inner().data_type(),
            Self::Stream(reader) => reader.data_type().clone(),
        }
    }
}

impl<'a> FromPyObject<'a> for AnyGeoArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(arr) = ob.extract() {
            Ok(Self::Array(arr))
        } else if let Ok(stream) = ob.extract() {
            Ok(Self::Stream(stream))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method.",
            ))
        }
    }
}
