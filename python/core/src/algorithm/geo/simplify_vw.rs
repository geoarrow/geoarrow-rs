use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use crate::ffi::to_python::geometry_array_to_pyobject;
use geoarrow::algorithm::geo::SimplifyVw;
use geoarrow::array::from_arrow_array;
use pyo3::prelude::*;

/// Returns the simplified representation of a geometry, using the
/// [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
/// algorithm
///
/// See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
///
/// Polygons are simplified by running the algorithm on all their constituent rings.
/// This may result in invalid Polygons, and has no guarantee of preserving topology.
/// Multi* objects are simplified by simplifying all their constituent geometries
/// individually.
///
/// An epsilon less than or equal to zero will return an unaltered version of the
/// geometry.
///
/// Args:
///     input: input geometry array
///     epsilon: tolerance for simplification.
///
/// Returns:
///     Simplified geometry array.
#[pyfunction]
pub fn simplify_vw(input: &PyAny, epsilon: f64) -> PyGeoArrowResult<PyObject> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    let result = array.as_ref().simplify_vw(&epsilon)?;
    Python::with_gil(|py| geometry_array_to_pyobject(py, result))
}

macro_rules! impl_simplify_vw {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Returns the simplified representation of a geometry, using the
            /// [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
            /// algorithm
            ///
            /// See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
            ///
            /// Polygons are simplified by running the algorithm on all their constituent rings.
            /// This may result in invalid Polygons, and has no guarantee of preserving topology.
            /// Multi* objects are simplified by simplifying all their constituent geometries
            /// individually.
            ///
            /// An epsilon less than or equal to zero will return an unaltered version of the
            /// geometry.
            ///
            /// Args:
            ///     epsilon: tolerance for simplification.
            ///
            /// Returns:
            ///     Simplified geometry array.
            pub fn simplify_vw(&self, epsilon: f64) -> Self {
                SimplifyVw::simplify_vw(&self.0, &epsilon).into()
            }
        }
    };
}

impl_simplify_vw!(PointArray);
impl_simplify_vw!(LineStringArray);
impl_simplify_vw!(PolygonArray);
impl_simplify_vw!(MultiPointArray);
impl_simplify_vw!(MultiLineStringArray);
impl_simplify_vw!(MultiPolygonArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Returns the simplified representation of a geometry, using the
            /// [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
            /// algorithm
            ///
            /// See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
            ///
            /// Polygons are simplified by running the algorithm on all their constituent rings.
            /// This may result in invalid Polygons, and has no guarantee of preserving topology.
            /// Multi* objects are simplified by simplifying all their constituent geometries
            /// individually.
            ///
            /// An epsilon less than or equal to zero will return an unaltered version of the
            /// geometry.
            ///
            /// Args:
            ///     epsilon: tolerance for simplification.
            ///
            /// Returns:
            ///     Simplified geometry array.
            pub fn simplify_vw(&self, epsilon: f64) -> Self {
                SimplifyVw::simplify_vw(&self.0, &epsilon).into()
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
