use arrow::datatypes::DataType;
use geoarrow::ArrayBase;
use geoarrow::array::WKTArray;
use geoarrow::chunked_array::{ChunkedNativeArrayDyn, ChunkedWKTArray};
use geoarrow::io::wkt::{ToWKT, read_wkt};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyChunkedArray};

use crate::ffi::from_python::AnyNativeInput;
use crate::ffi::to_python::{chunked_native_array_to_pyobject, native_array_to_pyobject};
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
            let geo_array = match array.data_type() {
                DataType::Utf8 => {
                    let wkt_arr = WKTArray::<i32>::try_from((array.as_ref(), field.as_ref()))?;
                    read_wkt(&wkt_arr, coord_type, false)?
                }
                DataType::LargeUtf8 => {
                    let wkt_arr = WKTArray::<i64>::try_from((array.as_ref(), field.as_ref()))?;
                    read_wkt(&wkt_arr, coord_type, false)?
                }
                other => {
                    return Err(
                        PyTypeError::new_err(format!("Unexpected array type {:?}", other)).into(),
                    );
                }
            };
            native_array_to_pyobject(py, geo_array)
        }
        AnyArray::Stream(s) => {
            let chunked_arr = s.into_chunked_array()?;
            let (chunks, field) = chunked_arr.into_inner();
            let geo_array = match field.data_type() {
                DataType::Utf8 => {
                    let wkt_chunks = chunks
                        .iter()
                        .map(|chunk| WKTArray::<i32>::try_from((chunk.as_ref(), field.as_ref())))
                        .collect::<Result<Vec<_>, _>>()?;
                    let ca = ChunkedWKTArray::new(wkt_chunks);
                    let parsed = ca.try_map(|chunk| read_wkt(chunk, coord_type, false))?;
                    let parsed_refs = parsed.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                    ChunkedNativeArrayDyn::from_geoarrow_chunks(parsed_refs.as_ref())?
                }
                DataType::LargeUtf8 => {
                    let wkt_chunks = chunks
                        .iter()
                        .map(|chunk| WKTArray::<i64>::try_from((chunk.as_ref(), field.as_ref())))
                        .collect::<Result<Vec<_>, _>>()?;
                    let ca = ChunkedWKTArray::new(wkt_chunks);
                    let parsed = ca.try_map(|chunk| read_wkt(chunk, coord_type, false))?;
                    let parsed_refs = parsed.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                    ChunkedNativeArrayDyn::from_geoarrow_chunks(parsed_refs.as_ref())?
                }
                other => {
                    return Err(
                        PyTypeError::new_err(format!("Unexpected array type {:?}", other)).into(),
                    );
                }
            };
            chunked_native_array_to_pyobject(py, geo_array.into_inner())
        }
    }
}

#[pyfunction]
pub fn to_wkt(py: Python, input: AnyNativeInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(array) => {
            let wkt_arr = array.as_ref().to_wkt::<i32>()?;
            let field = wkt_arr.extension_field();
            return_array(py, PyArray::new(wkt_arr.into_array_ref(), field))
        }
        AnyNativeInput::Chunked(array) => {
            let out = array.as_ref().to_wkt::<i32>()?;
            let field = out.extension_field();
            let chunks = out
                .into_inner()
                .into_iter()
                .map(|chunk| chunk.to_array_ref())
                .collect();
            return_chunked_array(py, PyChunkedArray::try_new(chunks, field)?)
        }
    }
}

pub(crate) fn return_array(py: Python, arr: PyArray) -> PyGeoArrowResult<PyObject> {
    Ok(arr.to_arro3(py)?.unbind())
}

pub(crate) fn return_chunked_array(py: Python, arr: PyChunkedArray) -> PyGeoArrowResult<PyObject> {
    Ok(arr.to_arro3(py)?.unbind())
}
