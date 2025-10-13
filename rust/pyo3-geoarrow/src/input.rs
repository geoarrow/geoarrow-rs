use geoarrow_array::{GeoArrowArrayIterator, GeoArrowArrayReader};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;

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
        let reader = self.into_reader()?;
        let data_type = reader.data_type();
        let chunks = reader.collect::<GeoArrowResult<Vec<_>>>()?;
        Ok(PyGeoChunkedArray::try_new(chunks, data_type)?)
    }

    pub fn into_reader(self) -> PyResult<Box<dyn GeoArrowArrayReader + Send>> {
        match self {
            Self::Array(array) => {
                let geo_array = array.into_inner();
                let data_type = geo_array.data_type();
                Ok(Box::new(GeoArrowArrayIterator::new(
                    vec![Ok(geo_array)],
                    data_type,
                )))
            }
            Self::Stream(stream) => stream.into_reader(),
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
        // First extract infallibly if __arrow_c_array__ method is present, so that any exception
        // in that gets propagated. Also check if PyArray extract works so that Buffer Protocol
        // conversion still works.
        // Do the same for __arrow_c_stream__ and PyArrayReader below.
        if ob.hasattr(intern!(ob.py(), "__arrow_c_array__"))? {
            Ok(Self::Array(ob.extract()?))
        } else if ob.hasattr(intern!(ob.py(), "__arrow_c_stream__"))? {
            Ok(Self::Stream(ob.extract()?))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method.",
            ))
        }
    }
}
