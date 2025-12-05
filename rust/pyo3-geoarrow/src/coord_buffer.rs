use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::DataType;
use geoarrow_array::array::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
use geoarrow_schema::Dimension;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use pyo3_arrow::PyArray;

use crate::PyGeoArrowError;

/// Python wrapper for a GeoArrow coordinate buffer.
///
/// Coordinate buffers store the raw coordinate data for geometries. They can be in either
/// separated format (separate arrays for x, y, z, m) or interleaved format (single array
/// with coordinates interleaved).
pub struct PyCoordBuffer(CoordBuffer);

impl PyCoordBuffer {
    /// Consume this wrapper and return the underlying coordinate buffer.
    pub fn into_inner(self) -> CoordBuffer {
        self.0
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyCoordBuffer {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if ob.is_instance_of::<PyTuple>() || ob.is_instance_of::<PyList>() {
            let arrays = ob.extract::<Vec<PyArray>>()?;

            if arrays.len() < 2 || arrays.len() > 3 {
                return Err(PyValueError::new_err(format!(
                    "Expected 2 or 3 arrays for each dimension, got {}.",
                    arrays.len()
                )));
            }

            let x = arrays[0].array();
            let y = arrays[1].array();

            if !matches!(x.data_type(), DataType::Float64) {
                return Err(PyValueError::new_err(format!(
                    "Expected x to be float64 data type, got {}",
                    x.data_type()
                )));
            }

            if !matches!(y.data_type(), DataType::Float64) {
                return Err(PyValueError::new_err(format!(
                    "Expected y to be float64 data type, got {}",
                    y.data_type()
                )));
            }

            let x = x.as_primitive::<Float64Type>();
            let y = y.as_primitive::<Float64Type>();

            if x.null_count() != 0 {
                return Err(PyValueError::new_err(format!(
                    "Cannot construct point array with null values. The 'x' array has {} null values",
                    x.null_count()
                )));
            }

            if y.null_count() != 0 {
                return Err(PyValueError::new_err(format!(
                    "Cannot construct point array with null values. The 'y' array has {} null values",
                    y.null_count()
                )));
            }

            let x = x.values();
            let y = y.values();

            if let Some(z) = arrays.get(2) {
                if !matches!(z.field().data_type(), DataType::Float64) {
                    return Err(PyValueError::new_err(format!(
                        "Expected z to be float64 data type, got {}",
                        z.field().data_type()
                    )));
                }

                let z = z.array().as_primitive::<Float64Type>();

                if z.null_count() != 0 {
                    return Err(PyValueError::new_err(format!(
                        "Cannot construct point array with null values. The 'z' array has {} null values",
                        z.null_count()
                    )));
                }

                let z = z.values();

                Ok(Self(
                    SeparatedCoordBuffer::from_vec(
                        vec![x.clone(), y.clone(), z.clone()],
                        Dimension::XYZ,
                    )
                    .map_err(PyGeoArrowError::from)?
                    .into(),
                ))
            } else {
                Ok(Self(
                    SeparatedCoordBuffer::from_vec(vec![x.clone(), y.clone()], Dimension::XY)
                        .map_err(PyGeoArrowError::from)?
                        .into(),
                ))
            }
        } else {
            let coords = ob.extract::<PyArray>()?;

            match coords.field().data_type() {
                DataType::FixedSizeList(inner_field, list_size) => {
                    if !matches!(inner_field.data_type(), DataType::Float64) {
                        return Err(PyValueError::new_err(format!(
                            "Expected the inner field of coords to be float64 data type, got {}",
                            inner_field.data_type()
                        )));
                    }

                    let coords = coords.as_ref().as_fixed_size_list();

                    if coords.null_count() != 0 {
                        return Err(PyValueError::new_err(format!(
                            "Cannot have null values in coordinate fixed size list array. {} null values present.",
                            coords.null_count()
                        )));
                    }

                    let values = coords.values();
                    let values = values.as_primitive::<Float64Type>();

                    if values.null_count() != 0 {
                        return Err(PyValueError::new_err(format!(
                            "Cannot construct point array with null values in the inner buffer of the coordinate array. The values of the fixed size list array array has {} null values",
                            values.null_count()
                        )));
                    }

                    match list_size {
                        2 => Ok(Self(
                            InterleavedCoordBuffer::new(values.values().clone(), Dimension::XY)
                                .into(),
                        )),
                        3 => Ok(Self(
                            InterleavedCoordBuffer::new(values.values().clone(), Dimension::XYZ)
                                .into(),
                        )),
                        _ => Err(PyValueError::new_err(format!(
                            "Unsupported fixed size list size {list_size}",
                        ))),
                    }
                }
                dt => Err(PyValueError::new_err(format!(
                    "Expected coords to be FixedSizeList data type, got {dt}",
                ))),
            }
        }
    }
}
