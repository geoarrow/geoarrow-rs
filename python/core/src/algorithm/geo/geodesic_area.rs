use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::GeodesicArea;
use pyo3::prelude::*;

/// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)].
///
/// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
/// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
///
/// ## Units
///
/// - return value: meter
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
///
/// Returns:
///     Array with output values.
#[pyfunction]
pub fn geodesic_perimeter(input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = Float64Array::from(arr.as_ref().geodesic_perimeter()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = ChunkedFloat64Array::from(arr.as_ref().geodesic_perimeter()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
    }
}

macro_rules! impl_geodesic_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
            /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
            ///
            /// ## Units
            ///
            /// - return value: meter
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_perimeter(&self) -> Float64Array {
                GeodesicArea::geodesic_perimeter(&self.0).into()
            }
        }
    };
}

impl_geodesic_area!(PointArray);
impl_geodesic_area!(LineStringArray);
impl_geodesic_area!(PolygonArray);
impl_geodesic_area!(MultiPointArray);
impl_geodesic_area!(MultiLineStringArray);
impl_geodesic_area!(MultiPolygonArray);
impl_geodesic_area!(MixedGeometryArray);
impl_geodesic_area!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)].
            ///
            /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
            /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
            ///
            /// ## Units
            ///
            /// - return value: meter
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            ///
            /// Returns:
            ///     Array with output values.
            pub fn geodesic_perimeter(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(GeodesicArea::geodesic_perimeter(&self.0)?.into())
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
