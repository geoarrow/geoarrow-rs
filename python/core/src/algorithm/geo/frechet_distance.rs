use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{FrechetDistance, FrechetDistanceLineString};
use geoarrow::algorithm::native::as_chunked_geometry_array;
use geoarrow::array::{AsChunkedGeometryArray, AsGeometryArray};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::geo::geometry_to_geo;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
pub fn frechet_distance(
    input: AnyGeometryInput,
    other: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            let result = Float64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Array(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            let result = Float64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        // TODO: Unknown whether this should be supported. I like "array in, array out".
        // (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Chunked(right)) => {
        //     let left_chunked = as_chunked_geometry_array(&left.as_ref(), &right.chunk_lengths())?;
        //     let result = FrechetDistance::frechet_distance(&left_chunked.as_ref(), &right.as_ref())?;
        //     let result = Float64Array::from(result);
        //     Python::with_gil(|py| Ok(result.into_py(py)))
        // }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Array(right)) => {
            let right_chunked = as_chunked_geometry_array(right.as_ref(), &left.chunk_lengths())?;
            let result =
                FrechetDistance::frechet_distance(&left.as_ref(), &right_chunked.as_ref())?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Chunked(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}

#[pymethods]
impl LineStringArray {
    pub fn frechet_distance(
        &self,
        other: AnyGeometryBroadcastInput,
    ) -> PyGeoArrowResult<Float64Array> {
        match other {
            AnyGeometryBroadcastInput::Array(right) => {
                let result = match right.data_type() {
                    GeoDataType::LineString(_) => {
                        FrechetDistance::frechet_distance(&self.0, right.as_ref().as_line_string())
                    }
                    GeoDataType::LargeLineString(_) => FrechetDistance::frechet_distance(
                        &self.0,
                        right.as_ref().as_large_line_string(),
                    ),
                    dt => {
                        return Err(PyValueError::new_err(format!(
                            "Unsupported broadcast type {:?}",
                            dt
                        ))
                        .into());
                    }
                };
                Ok(result.into())
            }
            AnyGeometryBroadcastInput::Scalar(right) => {
                let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                    .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
                let result = FrechetDistanceLineString::frechet_distance(&self.0, &scalar);
                Ok(result.into())
            }
            _ => Err(PyValueError::new_err("Unsupported broadcast type.").into()),
        }
    }
}

#[pymethods]
impl ChunkedLineStringArray {
    pub fn frechet_distance(
        &self,
        other: AnyGeometryBroadcastInput,
    ) -> PyGeoArrowResult<ChunkedFloat64Array> {
        match other {
            AnyGeometryBroadcastInput::Chunked(right) => {
                let result = match right.data_type() {
                    GeoDataType::LineString(_) => {
                        FrechetDistance::frechet_distance(&self.0, right.as_ref().as_line_string())
                    }
                    GeoDataType::LargeLineString(_) => FrechetDistance::frechet_distance(
                        &self.0,
                        right.as_ref().as_large_line_string(),
                    ),
                    dt => {
                        return Err(PyValueError::new_err(format!(
                            "Unsupported broadcast type {:?}",
                            dt
                        ))
                        .into());
                    }
                };
                Ok(result.into())
            }
            AnyGeometryBroadcastInput::Scalar(right) => {
                let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                    .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
                let result = FrechetDistanceLineString::frechet_distance(&self.0, &scalar);
                Ok(result.into())
            }
            _ => Err(PyValueError::new_err("Unsupported broadcast type.").into()),
        }
    }
}
