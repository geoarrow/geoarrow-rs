use std::sync::Arc;

use crate::ffi::from_python::utils::{import_arrow_c_array, import_arrow_c_stream};
use crate::ffi::stream_chunked::ArrowArrayStreamReader;
use arrow_array::Array;
use geoarrow::array::from_arrow_array;
use geoarrow::chunked_array::{from_arrow_chunks, ChunkedGeometryArrayTrait};
use geoarrow::io::geozero::ToGeometry;
use geoarrow::GeometryArrayTrait;
use geozero::geojson::GeoJsonString;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{intern, PyAny, PyResult};

pub struct ArrayInput(pub Arc<dyn Array>);

impl<'a> FromPyObject<'a> for ArrayInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let (array, _field) = import_arrow_c_array(ob)?;
        Ok(Self(array))
    }
}

pub struct ChunkedArrayInput(pub Vec<Arc<dyn Array>>);

impl<'a> FromPyObject<'a> for ChunkedArrayInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let stream = import_arrow_c_stream(ob)?;
        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        let mut chunks = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            chunks.push(batch);
        }
        Ok(Self(chunks))
    }
}

pub enum AnyArrayInput {
    Array(Arc<dyn Array>),
    Chunked(Vec<Arc<dyn Array>>),
}

impl<'a> FromPyObject<'a> for AnyArrayInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            Ok(Self::Array(ArrayInput::extract(ob)?.0))
        } else if ob.hasattr("__arrow_c_stream__")? {
            Ok(Self::Chunked(ChunkedArrayInput::extract(ob)?.0))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

pub struct GeometryScalarInput(pub geoarrow::scalar::OwnedGeometry<i32>);

// TODO: deduplicate this with `FromPyObject` impls on the Python scalar classes
impl<'a> FromPyObject<'a> for GeometryScalarInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if let Ok((array, field)) = import_arrow_c_array(ob) {
            let _array = from_arrow_array(&array, &field)
                .map_err(|err| PyTypeError::new_err(err.to_string()))?;
            todo!("Downcast from array, assert length one, convert to OwnedGeometry");
        } else if ob.hasattr("__geo_interface__")? {
            // Load from geo interface
            let py_obj = ob.getattr("__geo_interface__")?;
            Python::with_gil(|py| {
                // Import JSON module
                let json_mod = py.import(intern!(py, "json"))?;

                // Prepare json.dumps call
                let args = (py_obj,);
                let separators = PyTuple::new(py, vec![',', ':']);
                let kwargs = PyDict::new(py);
                kwargs.set_item("separators", separators)?;

                // Call json.dumps
                let json_dumped = json_mod.call_method(intern!(py, "dumps"), args, Some(kwargs))?;
                let json_string = json_dumped.extract::<String>()?;

                // Parse GeoJSON to geometry scalar
                let reader = GeoJsonString(json_string);
                let geom = ToGeometry::<i32>::to_geometry(&reader).map_err(|err| {
                    PyValueError::new_err(format!("Unable to parse GeoJSON String: {}", err))
                })?;
                Ok(Self(geom))
            })
        } else {
            Err(PyValueError::new_err(
                "Expected GeoArrow scalar or object implementing Geo Interface.",
            ))
        }
    }
}

pub struct GeometryArrayInput(pub Arc<dyn GeometryArrayTrait>);

impl<'a> FromPyObject<'a> for GeometryArrayInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let (array, field) = import_arrow_c_array(ob)?;
        let array = from_arrow_array(&array, &field)
            .map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self(array))
    }
}

pub struct ChunkedGeometryArrayInput(pub Arc<dyn ChunkedGeometryArrayTrait>);

impl<'a> FromPyObject<'a> for ChunkedGeometryArrayInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let stream = import_arrow_c_stream(ob)?;
        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let field = stream_reader.field();

        let mut chunks = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            chunks.push(batch);
        }

        let chunk_refs = chunks
            .iter()
            .map(|chunk| chunk.as_ref())
            .collect::<Vec<_>>();
        let chunked_array = from_arrow_chunks(&chunk_refs, &field)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(chunked_array))
    }
}

pub enum AnyGeometryInput {
    Array(Arc<dyn GeometryArrayTrait>),
    Chunked(Arc<dyn ChunkedGeometryArrayTrait>),
}

impl<'a> FromPyObject<'a> for AnyGeometryInput {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            Ok(Self::Array(GeometryArrayInput::extract(ob)?.0))
        } else if ob.hasattr("__arrow_c_stream__")? {
            Ok(Self::Chunked(ChunkedGeometryArrayInput::extract(ob)?.0))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}