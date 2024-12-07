use geoarrow::array::WKBArray;
use geoarrow::chunked_array::{ChunkedArrayBase, ChunkedWKBArray};
use geoarrow::datatypes::{Dimension, SerializedType};
use geoarrow::io::wkb::{to_wkb as _to_wkb, FromWKB, ToWKB};
use geoarrow::ArrayBase;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyChunkedArray};
use pyo3_geoarrow::PyCoordType;

use crate::ffi::from_python::AnyNativeInput;
use crate::ffi::to_python::{chunked_native_array_to_pyobject, native_array_to_pyobject};
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
#[pyo3(
    signature = (input, *, coord_type = PyCoordType::Interleaved),
    text_signature = "(input, *, coord_type = 'interleaved')")
]
pub fn from_wkb(
    py: Python,
    input: AnyArray,
    coord_type: PyCoordType,
) -> PyGeoArrowResult<PyObject> {
    let coord_type = coord_type.into();
    match input {
        AnyArray::Array(arr) => {
            let (arr, field) = arr.into_inner();
            let typ = SerializedType::try_from(field.as_ref())?;
            let geo_array = match typ {
                SerializedType::WKB => {
                    let wkb_arr = WKBArray::<i32>::try_from((arr.as_ref(), field.as_ref()))?;
                    FromWKB::from_wkb(&wkb_arr, coord_type, Dimension::XY)?
                }
                SerializedType::LargeWKB => {
                    let wkb_arr = WKBArray::<i64>::try_from((arr.as_ref(), field.as_ref()))?;
                    FromWKB::from_wkb(&wkb_arr, coord_type, Dimension::XY)?
                }
                _ => return Err(PyValueError::new_err("Expected a WKB array").into()),
            };
            native_array_to_pyobject(py, geo_array)
        }
        AnyArray::Stream(s) => {
            let (chunks, field) = s.into_chunked_array()?.into_inner();
            let typ = SerializedType::try_from(field.as_ref())?;
            let geo_array = match typ {
                SerializedType::WKB => {
                    let chunks = chunks
                        .into_iter()
                        .map(|chunk| WKBArray::<i32>::try_from((chunk.as_ref(), field.as_ref())))
                        .collect::<Result<Vec<_>, _>>()?;
                    FromWKB::from_wkb(&ChunkedWKBArray::new(chunks), coord_type, Dimension::XY)?
                }
                SerializedType::LargeWKB => {
                    let chunks = chunks
                        .into_iter()
                        .map(|chunk| WKBArray::<i64>::try_from((chunk.as_ref(), field.as_ref())))
                        .collect::<Result<Vec<_>, _>>()?;
                    FromWKB::from_wkb(&ChunkedWKBArray::new(chunks), coord_type, Dimension::XY)?
                }
                _ => return Err(PyValueError::new_err("Expected a WKB array").into()),
            };
            chunked_native_array_to_pyobject(py, geo_array)
        }
    }
}

#[pyfunction]
pub fn to_wkb(py: Python, input: AnyNativeInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let wkb_arr = _to_wkb::<i32>(arr.as_ref());
            let field = wkb_arr.extension_field();
            Ok(PyArray::new(wkb_arr.into_array_ref(), field)
                .to_arro3(py)?
                .unbind())
        }
        AnyNativeInput::Chunked(s) => {
            let out = s.as_ref().to_wkb::<i32>();
            let field = out.extension_field();
            Ok(PyChunkedArray::try_new(out.array_refs(), field)?
                .to_arro3(py)?
                .unbind())
        }
    }
}
