use arrow_schema::FieldRef;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::GeoArrowType;
use pyo3::exceptions::{PyIOError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::Arro3ArrayReader;
use pyo3_arrow::ffi::{ArrayIterator, ArrayReader, to_schema_pycapsule, to_stream_pycapsule};
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader, PyField};

use crate::data_type::PyGeoArrowType;
use crate::{PyGeoArray, PyGeoArrowError, PyGeoArrowResult, PyGeoChunkedArray};

/// A Python-facing GeoArrow array reader.
///
/// This is a wrapper around a [PyArrayReader].
#[pyclass(
    module = "geoarrow.rust.core",
    name = "GeoArrayReader",
    subclass,
    frozen
)]
pub struct PyGeoArrayReader(PyArrayReader, GeoArrowType);

impl PyGeoArrayReader {
    /// Construct a new [PyArrayReader] from an existing [ArrayReader].
    pub fn try_new(reader: PyArrayReader) -> PyGeoArrowResult<Self> {
        let field = reader.field_ref()?;
        let data_type = GeoArrowType::try_from(field.as_ref())?;
        Ok(Self(reader, data_type))
    }

    /// Import from a raw Arrow C Stream capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyGeoArrowResult<Self> {
        let reader = PyArrayReader::from_arrow_pycapsule(capsule)?;
        Self::try_new(reader)
    }

    pub fn into_inner(self) -> (PyArrayReader, GeoArrowType) {
        (self.0, self.1)
    }

    pub fn data_type(&self) -> &GeoArrowType {
        &self.1
    }

    /// Consume this reader and convert into a [ArrayReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(self) -> PyResult<Box<dyn ArrayReader + Send>> {
        self.0.into_reader()
    }

    /// Consume this reader and create a [PyGeoChunkedArray] object
    pub fn into_chunked_array(self) -> PyGeoArrowResult<PyGeoChunkedArray> {
        self.read_all()
    }

    /// Access the [FieldRef] of this ArrayReader.
    ///
    /// If the stream has already been consumed, this method will error.
    pub fn field_ref(&self) -> PyResult<FieldRef> {
        self.0.field_ref()
    }

    /// Export this to a Python `arro3.core.ArrayReader`.
    pub fn into_arro3(self) -> Arro3ArrayReader {
        self.0.into()
    }
}

impl TryFrom<Box<dyn ArrayReader + Send>> for PyGeoArrayReader {
    type Error = PyGeoArrowError;

    fn try_from(value: Box<dyn ArrayReader + Send>) -> Result<Self, Self::Error> {
        Self::try_new(value.into())
    }
}

impl TryFrom<PyArrayReader> for PyGeoArrayReader {
    type Error = PyGeoArrowError;

    fn try_from(value: PyArrayReader) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

#[pymethods]
impl PyGeoArrayReader {
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.field_ref()?.as_ref())
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let array_reader = self
            .0
            .as_ref()
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    // Return self
    // https://stackoverflow.com/a/52056290
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&self) -> PyGeoArrowResult<PyGeoArray> {
        self.read_next_array()
    }

    // fn __repr__(&self) -> String {
    //     self.to_string()
    // }

    #[getter]
    fn closed(&self) -> bool {
        self.0.as_ref().lock().unwrap().is_none()
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
        r#type: PyField,
        arrays: Vec<PyArray>,
    ) -> PyGeoArrowResult<Self> {
        let arrays = arrays
            .into_iter()
            .map(|array| {
                let (arr, _field) = array.into_inner();
                arr
            })
            .collect::<Vec<_>>();
        PyArrayReader::new(Box::new(ArrayIterator::new(
            arrays.into_iter().map(Ok),
            r#type.into_inner(),
        )))
        .try_into()
    }

    #[classmethod]
    fn from_stream(_cls: &Bound<PyType>, reader: PyArrayReader) -> PyGeoArrowResult<Self> {
        Self::try_new(reader)
    }

    #[getter]
    fn r#type(&self) -> PyGeoArrowType {
        self.1.clone().into()
    }

    fn read_all(&self) -> PyGeoArrowResult<PyGeoChunkedArray> {
        let (chunks, field) = self.0.to_chunked_array()?.into_inner();
        let geo_arrays = chunks
            .iter()
            .map(|array| from_arrow_array(array, &field))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(PyGeoChunkedArray::new(geo_arrays, self.1.clone()))
    }

    fn read_next_array(&self) -> PyGeoArrowResult<PyGeoArray> {
        let mut inner = self.0.as_ref().lock().unwrap();
        let stream = inner
            .as_mut()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;

        if let Some(next_array) = stream.next() {
            let array = from_arrow_array(&next_array?, &self.1.to_field("", true))?;
            Ok(PyGeoArray::new(array))
        } else {
            Err(PyStopIteration::new_err("").into())
        }
    }
}
