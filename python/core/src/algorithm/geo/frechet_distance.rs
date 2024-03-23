use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{FrechetDistance, FrechetDistanceLineString};
use geoarrow::array::{AsChunkedGeometryArray, AsGeometryArray};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::geo::geometry_to_geo;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Determine the similarity between two arrays of `LineStrings` using the [Frechet distance].
///
/// The Fréchet distance is a measure of similarity: it is the greatest distance between any point
/// in A and the closest point in B. The discrete distance is an approximation of this metric: only
/// vertices are considered. The parameter ‘densify’ makes this approximation less coarse by
/// splitting the line segments between vertices before computing the distance.
///
/// Fréchet distance sweep continuously along their respective curves and the direction of curves
/// is significant. This makes it a better measure of similarity than Hausdorff distance for curve
/// or surface matching.
///
///
/// This implementation is based on [Computing Discrete Frechet Distance] by T. Eiter and H.
/// Mannila.
///
/// [Frechet distance]: https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance
/// [Computing Discrete Frechet Distance]: http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
///
/// Args:
///     input: input geometry array or chunked geometry array
///     other: the geometry or geometry array to compare against. This must contain geometries of
///         `LineString`` type. A variety of inputs are accepted:
///
///         - A scalar [`LineString`][geoarrow.rust.core.LineString]
///         - A [`LineStringArray`][geoarrow.rust.core.LineStringArray]
///         - A [`ChunkedLineStringArray`][geoarrow.rust.core.ChunkedLineStringArray]
///         - Any Python class that implements the Geo Interface, such as a [`shapely` LineString][shapely.LineString]
///         - Any GeoArrow array or chunked array of `LineString` type
///
/// Returns:
///     Array or chunked array with float distance values.
#[pyfunction]
pub fn frechet_distance(
    input: AnyGeometryInput,
    other: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Array(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            let result = Float64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Chunked(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            let result = Float64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}

#[pymethods]
impl LineStringArray {
    /// Determine the similarity between two arrays of `LineStrings` using the [Frechet distance].
    ///
    /// The Fréchet distance is a measure of similarity: it is the greatest distance between any
    /// point in A and the closest point in B. The discrete distance is an approximation of this
    /// metric: only vertices are considered. The parameter ‘densify’ makes this approximation less
    /// coarse by splitting the line segments between vertices before computing the distance.
    ///
    /// Fréchet distance sweep continuously along their respective curves and the direction of
    /// curves is significant. This makes it a better measure of similarity than Hausdorff distance
    /// for curve or surface matching.
    ///
    ///
    /// This implementation is based on [Computing Discrete Frechet Distance] by T. Eiter and H.
    /// Mannila.
    ///
    /// [Frechet distance]: https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance
    /// [Computing Discrete Frechet Distance]:
    ///     http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
    ///
    /// Args:
    ///     other: the geometry or geometry array to compare against. This must contain geometries
    ///         of `LineString`` type. A variety of inputs are accepted:
    ///
    ///         - A scalar [`LineString`][geoarrow.rust.core.LineString]
    ///         - A [`LineStringArray`][geoarrow.rust.core.LineStringArray]
    ///         - A [`ChunkedLineStringArray`][geoarrow.rust.core.ChunkedLineStringArray]
    ///         - Any Python class that implements the Geo Interface, such as a [`shapely` LineString][shapely.LineString]
    ///         - Any GeoArrow array or chunked array of `LineString` type
    ///
    /// Returns:
    ///     Array with float distance values.
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
    /// Determine the similarity between two arrays of `LineStrings` using the [Frechet distance].
    ///
    /// The Fréchet distance is a measure of similarity: it is the greatest distance between any
    /// point in A and the closest point in B. The discrete distance is an approximation of this
    /// metric: only vertices are considered. The parameter ‘densify’ makes this approximation less
    /// coarse by splitting the line segments between vertices before computing the distance.
    ///
    /// Fréchet distance sweep continuously along their respective curves and the direction of
    /// curves is significant. This makes it a better measure of similarity than Hausdorff distance
    /// for curve or surface matching.
    ///
    ///
    /// This implementation is based on [Computing Discrete Frechet Distance] by T. Eiter and H.
    /// Mannila.
    ///
    /// [Frechet distance]: https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance
    /// [Computing Discrete Frechet Distance]:
    ///     http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
    ///
    /// Args:
    ///     other: the geometry or geometry array to compare against. This must contain geometries
    ///         of `LineString`` type. A variety of inputs are accepted:
    ///
    ///         - A scalar [`LineString`][geoarrow.rust.core.LineString]
    ///         - A [`LineStringArray`][geoarrow.rust.core.LineStringArray]
    ///         - A [`ChunkedLineStringArray`][geoarrow.rust.core.ChunkedLineStringArray]
    ///         - Any Python class that implements the Geo Interface, such as a [`shapely` LineString][shapely.LineString]
    ///         - Any GeoArrow array or chunked array of `LineString` type
    ///
    /// Returns:
    ///     Chunked Array with float distance values.
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
