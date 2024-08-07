use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::Densify as _Densify;

/// Return a new linear geometry containing both existing and new interpolated coordinates with
/// a maximum distance of `max_distance` between them.
///
/// Note: `max_distance` must be greater than 0.
///
/// # Examples
/// ```
/// use geo::{coord, Line, LineString};
/// use geo::Densify;
///
/// let line: Line<f64> = Line::new(coord! {x: 0.0, y: 6.0}, coord! {x: 1.0, y: 8.0});
/// let correct: LineString<f64> = vec![[0.0, 6.0], [0.5, 7.0], [1.0, 8.0]].into();
/// let max_dist = 2.0;
/// let densified = line.densify(max_dist);
/// assert_eq!(densified, correct);
///```
pub trait Densify {
    type Output;

    fn densify(&self, max_distance: f64) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> Densify for $type {
            type Output = $type;

            fn densify(&self, max_distance: f64) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.densify(max_distance)))
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

impl Densify for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::LineString(_, Dimension::XY) => {
                Arc::new(self.as_line_string_2d().densify(max_distance))
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_line_string_2d().densify(max_distance))
            }
            GeoDataType::Polygon(_, Dimension::XY) => {
                Arc::new(self.as_polygon_2d().densify(max_distance))
            }
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_polygon_2d().densify(max_distance))
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_multi_line_string_2d().densify(max_distance))
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_line_string_2d().densify(max_distance))
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_multi_polygon_2d().densify(max_distance))
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_polygon_2d().densify(max_distance))
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait> Densify for $struct_name {
            type Output = $struct_name;

            fn densify(&self, max_distance: f64) -> Self::Output {
                self.map(|chunk| chunk.densify(max_distance))
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

impl Densify for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::LineString(_, Dimension::XY) => {
                Arc::new(self.as_line_string_2d().densify(max_distance))
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_line_string_2d().densify(max_distance))
            }
            GeoDataType::Polygon(_, Dimension::XY) => {
                Arc::new(self.as_polygon_2d().densify(max_distance))
            }
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_polygon_2d().densify(max_distance))
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_multi_line_string_2d().densify(max_distance))
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_line_string_2d().densify(max_distance))
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_multi_polygon_2d().densify(max_distance))
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                Arc::new(self.as_large_multi_polygon_2d().densify(max_distance))
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
