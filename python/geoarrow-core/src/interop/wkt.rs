use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::cast::AsArray;
use geoarrow::array::metadata::ArrayMetadata;
use geoarrow::chunked_array::{ChunkedArray, ChunkedMixedGeometryArray};
use geoarrow::io::geozero::FromWKT;
use geoarrow::io::wkt::reader::ParseWKT;
use geoarrow::io::wkt::ToWKT;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyChunkedArray};

use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use pyo3_geoarrow::{PyCoordType, PyGeoArrowResult};

#[pyfunction]
#[pyo3(
    signature = (input, *, coord_type = PyCoordType::Interleaved),
    text_signature = "(input, *, method = 'interleaved')")
]
pub fn from_wkt(
    py: Python,
    input: AnyArray,
    coord_type: PyCoordType,
) -> PyGeoArrowResult<PyObject> {
    let coord_type = coord_type.into();
    match input {
        AnyArray::Array(arr) => {
            let (array, field) = arr.into_inner();
            let metadata = Arc::new(ArrayMetadata::try_from(field.as_ref())?);
            let geo_array = match array.data_type() {
                DataType::Utf8 => array.as_string::<i32>().parse_wkt(coord_type, metadata),
                DataType::LargeUtf8 => array.as_string::<i64>().parse_wkt(coord_type, metadata),
                other => {
                    return Err(
                        PyTypeError::new_err(format!("Unexpected array type {:?}", other)).into(),
                    )
                }
            };
            geometry_array_to_pyobject(py, geo_array)
        }
        AnyArray::Stream(s) => {
            let chunked_arr = s.into_chunked_array()?;
            let (chunks, field) = chunked_arr.into_inner();
            let metadata = Arc::new(ArrayMetadata::try_from(field.as_ref())?);
            let geo_array: ChunkedMixedGeometryArray<i32, 2> = match field.data_type() {
                DataType::Utf8 => {
                    let string_chunks = chunks
                        .iter()
                        .map(|chunk| chunk.as_string::<i32>().clone())
                        .collect::<Vec<_>>();
                    FromWKT::from_wkt(
                        &ChunkedArray::new(string_chunks),
                        coord_type,
                        metadata,
                        false,
                    )?
                }
                DataType::LargeUtf8 => {
                    let string_chunks = chunks
                        .iter()
                        .map(|chunk| chunk.as_string::<i64>().clone())
                        .collect::<Vec<_>>();
                    FromWKT::from_wkt(
                        &ChunkedArray::new(string_chunks),
                        coord_type,
                        metadata,
                        false,
                    )?
                }
                other => {
                    return Err(
                        PyTypeError::new_err(format!("Unexpected array type {:?}", other)).into(),
                    )
                }
            };
            chunked_geometry_array_to_pyobject(py, Arc::new(geo_array))
        }
    }
}

#[pyfunction]
pub fn to_wkt(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(array) => return_array(
            py,
            PyArray::from_array_ref(Arc::new(array.as_ref().to_wkt::<i32>())),
        ),
        AnyGeometryInput::Chunked(array) => {
            let out = array.as_ref().to_wkt::<i32>();
            return_chunked_array(py, PyChunkedArray::from_array_refs(out.chunk_refs())?)
        }
    }
}

pub(crate) fn return_array(py: Python, arr: PyArray) -> PyGeoArrowResult<PyObject> {
    Ok(arr.to_arro3(py)?.to_object(py))
}

pub(crate) fn return_chunked_array(py: Python, arr: PyChunkedArray) -> PyGeoArrowResult<PyObject> {
    Ok(arr.to_arro3(py)?.to_object(py))
}
