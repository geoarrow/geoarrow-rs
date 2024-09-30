use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::ChaikinSmoothing as _ChaikinSmoothing;

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
pub trait ChaikinSmoothing {
    type Output;

    /// create a new geometry with the Chaikin smoothing being
    /// applied `n_iterations` times.
    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl ChaikinSmoothing for $type {
            type Output = Self;

            fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| {
                        maybe_g.map(|geom| geom.chaikin_smoothing(n_iterations.try_into().unwrap()))
                    })
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>, geo::LineString);
iter_geo_impl!(PolygonArray<2>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<2>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<2>, geo::MultiPolygon);

impl ChaikinSmoothing for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            LineString(_, XY) => {
                Arc::new(self.as_line_string::<2>().chaikin_smoothing(n_iterations))
            }
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().chaikin_smoothing(n_iterations)),
            MultiLineString(_, XY) => Arc::new(
                self.as_multi_line_string::<2>()
                    .chaikin_smoothing(n_iterations),
            ),
            MultiPolygon(_, XY) => {
                Arc::new(self.as_multi_polygon::<2>().chaikin_smoothing(n_iterations))
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($chunked_array:ty) => {
        impl ChaikinSmoothing for $chunked_array {
            type Output = Self;

            fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
                self.map(|chunk| chunk.chaikin_smoothing(n_iterations.into()))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<2>);
impl_chunked!(ChunkedPolygonArray<2>);
impl_chunked!(ChunkedMultiLineStringArray<2>);
impl_chunked!(ChunkedMultiPolygonArray<2>);

impl ChaikinSmoothing for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            LineString(_, XY) => {
                Arc::new(self.as_line_string::<2>().chaikin_smoothing(n_iterations))
            }
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().chaikin_smoothing(n_iterations)),
            MultiLineString(_, XY) => Arc::new(
                self.as_multi_line_string::<2>()
                    .chaikin_smoothing(n_iterations),
            ),
            MultiPolygon(_, XY) => {
                Arc::new(self.as_multi_polygon::<2>().chaikin_smoothing(n_iterations))
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
