use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::ConvexHull;
use pyo3::prelude::*;

/// Returns the convex hull of a Polygon. The hull is always oriented
/// counter-clockwise.
///
/// This implementation uses the QuickHull algorithm, based on [Barber, C. Bradford;
/// Dobkin, David P.; Huhdanpaa, Hannu (1 December
/// 1996)](https://dx.doi.org/10.1145%2F235815.235821) Original paper here:
/// <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with convex hull polygons.
#[pyfunction]
pub fn convex_hull(input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = PolygonArray::from(arr.as_ref().convex_hull()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = ChunkedPolygonArray::from(arr.as_ref().convex_hull()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
    }
}

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Returns the convex hull of a Polygon. The hull is always oriented
            /// counter-clockwise.
            ///
            /// This implementation uses the QuickHull algorithm, based on [Barber, C. Bradford;
            /// Dobkin, David P.; Huhdanpaa, Hannu (1 December
            /// 1996)](https://dx.doi.org/10.1145%2F235815.235821) Original paper here:
            /// <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>
            ///
            /// Returns:
            ///     Array with convex hull polygons.
            pub fn convex_hull(&self) -> PolygonArray {
                use geoarrow::algorithm::geo::ConvexHull;
                PolygonArray(ConvexHull::convex_hull(&self.0))
            }
        }
    };
}

impl_alg!(PointArray);
impl_alg!(LineStringArray);
impl_alg!(PolygonArray);
impl_alg!(MultiPointArray);
impl_alg!(MultiLineStringArray);
impl_alg!(MultiPolygonArray);
impl_alg!(MixedGeometryArray);
impl_alg!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Returns the convex hull of a Polygon. The hull is always oriented
            /// counter-clockwise.
            ///
            /// This implementation uses the QuickHull algorithm, based on [Barber, C. Bradford;
            /// Dobkin, David P.; Huhdanpaa, Hannu (1 December
            /// 1996)](https://dx.doi.org/10.1145%2F235815.235821) Original paper here:
            /// <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>
            ///
            /// Returns:
            ///     Array with convex hull polygons.
            pub fn convex_hull(&self) -> PyGeoArrowResult<ChunkedPolygonArray> {
                use geoarrow::algorithm::geo::ConvexHull;
                Ok(ChunkedPolygonArray(ConvexHull::convex_hull(&self.0)?))
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
