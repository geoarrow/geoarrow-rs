use crate::array::*;
use crate::chunked_array::*;
use pyo3::prelude::*;

macro_rules! impl_chaikin_smoothing {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
            ///
            /// [Chaikins smoothing algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)
            ///
            /// Each iteration of the smoothing doubles the number of vertices of the geometry, so in some
            /// cases it may make sense to apply a simplification afterwards to remove insignificant
            /// coordinates.
            ///
            /// This implementation preserves the start and end vertices of an open linestring and
            /// smoothes the corner between start and end of a closed linestring.
            pub fn chaikin_smoothing(&self, n_iterations: u32) -> Self {
                use geoarrow::algorithm::geo::ChaikinSmoothing;
                ChaikinSmoothing::chaikin_smoothing(&self.0, n_iterations).into()
            }
        }
    };
}

impl_chaikin_smoothing!(LineStringArray);
impl_chaikin_smoothing!(PolygonArray);
impl_chaikin_smoothing!(MultiLineStringArray);
impl_chaikin_smoothing!(MultiPolygonArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
            ///
            /// [Chaikins smoothing algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)
            ///
            /// Each iteration of the smoothing doubles the number of vertices of the geometry, so in some
            /// cases it may make sense to apply a simplification afterwards to remove insignificant
            /// coordinates.
            ///
            /// This implementation preserves the start and end vertices of an open linestring and
            /// smoothes the corner between start and end of a closed linestring.
            pub fn chaikin_smoothing(&self, n_iterations: u32) -> Self {
                use geoarrow::algorithm::geo::ChaikinSmoothing;
                ChaikinSmoothing::chaikin_smoothing(&self.0, n_iterations).into()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
