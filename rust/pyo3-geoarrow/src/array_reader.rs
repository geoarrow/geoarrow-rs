use std::sync::{Arc, Mutex};

use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArrayIterator, GeoArrowArrayReader};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;
use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3_arrow::PyArrayReader;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::{ArrayIterator, ArrayReader, to_schema_pycapsule, to_stream_pycapsule};
use pyo3_arrow::input::AnyArray;

use crate::data_type::PyGeoType;
use crate::utils::text_repr::text_repr;
use crate::{PyGeoArray, PyGeoArrowError, PyGeoArrowResult, PyGeoChunkedArray};

/// Python wrapper for a GeoArrow array reader (stream).
///
/// This type represents a stream of GeoArrow arrays that can be read incrementally. It implements
/// the Arrow C Stream Interface, allowing zero-copy data exchange with Arrow-compatible Python
/// libraries.
///
/// The reader can be iterated over to yield individual [`PyGeoArray`] chunks, or materialized
/// into a [`PyGeoChunkedArray`] using the [`into_chunked_array()`][Self::into_chunked_array]
/// method. For stream processing, prefer [`into_reader()`][Self::into_reader].
#[pyclass(
    module = "geoarrow.rust.core",
    name = "GeoArrayReader",
    subclass,
    frozen
)]
pub struct PyGeoArrayReader {
    iter: Mutex<Option<Box<dyn GeoArrowArrayReader + Send>>>,
    data_type: GeoArrowType,
}

impl PyGeoArrayReader {
    /// Create a new [`PyGeoArrayReader`] from a GeoArrow array reader.
    pub fn new(reader: Box<dyn GeoArrowArrayReader + Send>) -> Self {
        let data_type = reader.data_type();
        Self {
            iter: Mutex::new(Some(reader)),
            data_type,
        }
    }

    /// Import from a raw Arrow C Stream capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        let reader = PyArrayReader::from_arrow_pycapsule(capsule)?;
        Ok(Self::new(array_reader_to_geoarrow_array_reader(
            reader.into_reader()?,
        )?))
    }

    // pub fn into_inner(self) -> (PyArrayReader, GeoArrowType) {
    //     (self.iter, self.data_type)
    // }

    /// Get the GeoArrow data type of arrays in this stream.
    pub fn data_type(&self) -> &GeoArrowType {
        &self.data_type
    }

    /// Consume this reader and convert into a [ArrayReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(self) -> PyResult<Box<dyn GeoArrowArrayReader + Send>> {
        let stream = self
            .iter
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        Ok(stream)
    }

    /// Consume this reader and create a [PyGeoChunkedArray] object
    pub fn into_chunked_array(self) -> PyGeoArrowResult<PyGeoChunkedArray> {
        self.read_all()
    }

    /// Export to a geoarrow.rust.core.GeoArrowArrayReader.
    ///
    /// This requires that you depend on geoarrow-rust-core from your Python package.
    pub fn to_geoarrow_py<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let geoarrow_mod = py.import(intern!(py, "geoarrow.rust.core"))?;
        geoarrow_mod
            .getattr(intern!(py, "GeoArrayReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![self.__arrow_c_stream__(py, None)?])?,
            )
    }

    /// Export to a geoarrow.rust.core.GeoArrowArrayReader.
    ///
    /// This requires that you depend on geoarrow-rust-core from your Python package.
    pub fn into_geoarrow_py(self, py: Python) -> PyResult<Bound<PyAny>> {
        let geoarrow_mod = py.import(intern!(py, "geoarrow.rust.core"))?;
        let geoarray_reader = self
            .iter
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        let array_reader = geoarrow_array_reader_to_array_reader(geoarray_reader)?;
        let stream_pycapsule = to_stream_pycapsule(py, array_reader, None)?;
        geoarrow_mod
            .getattr(intern!(py, "GeoArrayReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![stream_pycapsule])?,
            )
    }
}

impl TryFrom<Box<dyn ArrayReader + Send>> for PyGeoArrayReader {
    type Error = PyGeoArrowError;

    fn try_from(value: Box<dyn ArrayReader + Send>) -> Result<Self, Self::Error> {
        Ok(Self::new(array_reader_to_geoarrow_array_reader(value)?))
    }
}

impl TryFrom<PyArrayReader> for PyGeoArrayReader {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArrayReader) -> Result<Self, Self::Error> {
        value.into_reader()?.try_into()
    }
}

#[pymethods]
impl PyGeoArrayReader {
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let field = self.data_type.to_field("", true);
        to_schema_pycapsule(py, field)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyGeoArrowResult<Bound<'py, PyCapsule>> {
        let geoarray_reader = self
            .iter
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        let array_reader = geoarrow_array_reader_to_array_reader(geoarray_reader)?;
        Ok(to_stream_pycapsule(py, array_reader, requested_schema)?)
    }

    // Return self
    // https://stackoverflow.com/a/52056290
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&self) -> PyGeoArrowResult<PyGeoArray> {
        self.read_next_array()
    }

    fn __repr__(&self) -> String {
        format!("GeoArrayReader({})", text_repr(self.data_type()))
    }

    #[getter]
    fn closed(&self) -> bool {
        self.iter.lock().unwrap().is_none()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyArray) -> PyGeoArrowResult<Self> {
        input.into_reader()?.try_into()
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyGeoArrowResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[classmethod]
    fn from_arrays(
        _cls: &Bound<PyType>,
        r#type: PyGeoType,
        arrays: Vec<PyGeoArray>,
    ) -> PyGeoArrowResult<Self> {
        let typ = r#type.into_inner();
        let arrays = arrays
            .into_iter()
            .map(|array| {
                let array = array.into_inner();
                if array.data_type() != typ {
                    return Err(PyValueError::new_err(format!(
                        "Array data type does not match expected type: got {:?}, expected {:?}",
                        array.data_type(),
                        typ
                    )));
                }
                Ok(array.to_array_ref())
            })
            .collect::<PyResult<Vec<_>>>()?;
        PyArrayReader::new(Box::new(ArrayIterator::new(
            arrays.into_iter().map(Ok),
            typ.to_field("", true).into(),
        )))
        .try_into()
    }

    #[classmethod]
    fn from_stream(_cls: &Bound<PyType>, reader: Self) -> Self {
        reader
    }

    #[getter]
    fn r#type(&self) -> PyGeoType {
        self.data_type.clone().into()
    }

    fn read_all(&self) -> PyGeoArrowResult<PyGeoChunkedArray> {
        let stream = self
            .iter
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let data_type = stream.data_type();
        let arrays = stream.collect::<GeoArrowResult<_>>()?;
        Ok(PyGeoChunkedArray::try_new(arrays, data_type)?)
    }

    fn read_next_array(&self) -> PyGeoArrowResult<PyGeoArray> {
        let mut inner = self.iter.lock().unwrap();
        let stream = inner
            .as_mut()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;

        if let Some(next_array) = stream.next() {
            Ok(PyGeoArray::new(next_array?))
        } else {
            Err(PyStopIteration::new_err("").into())
        }
    }
}

impl<'a> FromPyObject<'a> for PyGeoArrayReader {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let reader = ob.extract::<PyArrayReader>()?;
        Ok(Self::new(array_reader_to_geoarrow_array_reader(
            reader.into_reader()?,
        )?))
    }
}

fn array_reader_to_geoarrow_array_reader(
    reader: Box<dyn ArrayReader + Send>,
) -> PyGeoArrowResult<Box<dyn GeoArrowArrayReader + Send>> {
    let field = reader.field();
    let data_type = GeoArrowType::try_from(field.as_ref())?;
    let iter = reader
        .into_iter()
        .map(move |array| from_arrow_array(array?.as_ref(), field.as_ref()));
    Ok(Box::new(GeoArrowArrayIterator::new(iter, data_type)))
}

fn geoarrow_array_reader_to_array_reader(
    reader: Box<dyn GeoArrowArrayReader + Send>,
) -> PyGeoArrowResult<Box<dyn ArrayReader + Send>> {
    let field = Arc::new(reader.data_type().to_field("", true));
    let iter = reader
        .into_iter()
        .map(move |array| Ok(array?.to_array_ref()));
    Ok(Box::new(ArrayIterator::new(iter, field)))
}
