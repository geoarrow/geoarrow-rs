use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
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
        impl<O: OffsetSizeTrait> ChaikinSmoothing for $type {
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

iter_geo_impl!(LineStringArray<O, 2>, geo::LineString);
iter_geo_impl!(PolygonArray<O, 2>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<O, 2>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O, 2>, geo::MultiPolygon);

impl ChaikinSmoothing for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::LineString(_, Dimension::XY) => {
                Arc::new(self.as_line_string_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => Arc::new(
                self.as_large_line_string_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            GeoDataType::Polygon(_, Dimension::XY) => {
                Arc::new(self.as_polygon_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_polygon_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => Arc::new(
                self.as_multi_line_string_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => Arc::new(
                self.as_large_multi_line_string_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_multi_polygon_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => Arc::new(
                self.as_large_multi_polygon_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait> ChaikinSmoothing for $chunked_array {
            type Output = Self;

            fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
                self.map(|chunk| chunk.chaikin_smoothing(n_iterations.into()))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<O, 2>);
impl_chunked!(ChunkedPolygonArray<O, 2>);
impl_chunked!(ChunkedMultiLineStringArray<O, 2>);
impl_chunked!(ChunkedMultiPolygonArray<O, 2>);

impl ChaikinSmoothing for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn chaikin_smoothing(&self, n_iterations: u32) -> Self::Output {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::LineString(_, Dimension::XY) => {
                Arc::new(self.as_line_string_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => Arc::new(
                self.as_large_line_string_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            GeoDataType::Polygon(_, Dimension::XY) => {
                Arc::new(self.as_polygon_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_polygon_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => Arc::new(
                self.as_multi_line_string_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => Arc::new(
                self.as_large_multi_line_string_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_multi_polygon_2d().chaikin_smoothing(n_iterations))
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => Arc::new(
                self.as_large_multi_polygon_2d()
                    .chaikin_smoothing(n_iterations),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
