use std::sync::Arc;

use crate::scalar::PyGeometry;
use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float64Type};
use arrow_array::{Array, PrimitiveArray};
use arrow_buffer::ScalarBuffer;
use geoarrow::array::from_arrow_array;
use geoarrow::chunked_array::{from_arrow_chunks, ChunkedArray, ChunkedGeometryArrayTrait};
use geoarrow::GeometryArrayTrait;
use numpy::PyReadonlyArray1;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::PyArray;

// pub struct GeometryScalarInput(pub geoarrow::scalar::OwnedGeometry<i32, 2>);

// impl<'a> FromPyObject<'a> for GeometryScalarInput {
//     fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
//         Ok(Self(ob.extract::<PyGeometry>()?.0))
//     }
// }

pub struct GeometryArrayInput(pub Arc<dyn GeometryArrayTrait>);

impl<'a> FromPyObject<'a> for GeometryArrayInput {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let (array, field) = ob.extract::<PyArray>()?.into_inner();
        let array = from_arrow_array(&array, &field)
            .map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self(array))
    }
}

pub struct ChunkedGeometryArrayInput(pub Arc<dyn ChunkedGeometryArrayTrait>);

impl<'a> FromPyObject<'a> for ChunkedGeometryArrayInput {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let reader = ob.extract::<AnyArray>()?.into_reader()?;
        let field = reader.field();

        let mut chunks = vec![];
        for batch in reader {
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
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            Ok(Self::Array(GeometryArrayInput::extract_bound(ob)?.0))
        } else if ob.hasattr("__arrow_c_stream__")? {
            Ok(Self::Chunked(
                ChunkedGeometryArrayInput::extract_bound(ob)?.0,
            ))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

pub enum AnyGeometryBroadcastInput {
    Array(Arc<dyn GeometryArrayTrait>),
    Chunked(Arc<dyn ChunkedGeometryArrayTrait>),
    Scalar(Arc<PyGeometry>),
}

impl<'a> FromPyObject<'a> for AnyGeometryBroadcastInput {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(scalar) = ob.extract::<PyGeometry>() {
            Ok(Self::Scalar(Arc::new(scalar)))
        } else if ob.hasattr("__arrow_c_array__")? {
            Ok(Self::Array(GeometryArrayInput::extract_bound(ob)?.0))
        } else if ob.hasattr("__arrow_c_stream__")? {
            Ok(Self::Chunked(
                ChunkedGeometryArrayInput::extract_bound(ob)?.0,
            ))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __geo_interface__, __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

pub enum AnyPrimitiveBroadcastInput<T: ArrowPrimitiveType> {
    Array(PrimitiveArray<T>),
    Chunked(ChunkedArray<PrimitiveArray<T>>),
    Scalar(T::Native),
}

// TODO: Can we parametrize over all native types?
impl<'a> FromPyObject<'a> for AnyPrimitiveBroadcastInput<Float64Type> {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(scalar) = ob.extract::<f64>() {
            Ok(Self::Scalar(scalar))
        } else if let Ok(any_array) = ob.extract::<AnyArray>() {
            match any_array {
                AnyArray::Array(arr) => {
                    let float_arr = cast(arr.as_ref(), &DataType::Float64)
                        .map_err(|err| PyValueError::new_err(err.to_string()))?;
                    Ok(Self::Array(float_arr.as_primitive::<Float64Type>().clone()))
                }
                AnyArray::Stream(stream) => {
                    let chunks = stream.into_chunked_array()?;
                    let chunks = chunks
                        .chunks()
                        .iter()
                        .map(|chunk| {
                            let float_arr = cast(&chunk, &DataType::Float64)
                                .map_err(|err| PyValueError::new_err(err.to_string()))?;
                            Ok(float_arr.as_primitive::<Float64Type>().clone())
                        })
                        .collect::<Result<Vec<_>, PyErr>>()?;
                    Ok(Self::Chunked(ChunkedArray::new(chunks)))
                }
            }
        } else if ob.hasattr("__array__")? {
            let numpy_arr = ob.extract::<PyReadonlyArray1<f64>>()?;
            Ok(Self::Array(PrimitiveArray::from(
                numpy_arr.as_array().to_vec(),
            )))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __geo_interface__, __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

pub struct PyScalarBuffer<T: ArrowPrimitiveType>(pub ScalarBuffer<T::Native>);

impl<'a> FromPyObject<'a> for PyScalarBuffer<Float64Type> {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(array) = ob.extract::<PyArray>() {
            let float_arr = cast(array.as_ref(), &DataType::Float64)
                .map_err(|err| PyValueError::new_err(err.to_string()))?;
            if float_arr.null_count() > 0 {
                return Err(PyValueError::new_err(
                    "Cannot create scalar buffer from arrow array with nulls.",
                ));
            }

            Ok(Self(
                float_arr.as_primitive::<Float64Type>().values().clone(),
            ))
        } else if ob.hasattr("__array__")? {
            let numpy_arr = ob.extract::<PyReadonlyArray1<f64>>()?;
            Ok(Self(numpy_arr.as_array().to_vec().into()))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __array__",
            ))
        }
    }
}
