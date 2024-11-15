use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
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
        impl Densify for $type {
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

iter_geo_impl!(LineStringArray, geo::LineString);
iter_geo_impl!(PolygonArray, geo::Polygon);
iter_geo_impl!(MultiLineStringArray, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray, geo::MultiPolygon);

impl Densify for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            LineString(_, XY) => Arc::new(self.as_line_string().densify(max_distance)),
            Polygon(_, XY) => Arc::new(self.as_polygon().densify(max_distance)),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string().densify(max_distance))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().densify(max_distance)),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl Densify for $struct_name {
            type Output = $struct_name;

            fn densify(&self, max_distance: f64) -> Self::Output {
                self.map(|chunk| chunk.densify(max_distance))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);

impl Densify for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn densify(&self, max_distance: f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            LineString(_, XY) => Arc::new(self.as_line_string().densify(max_distance)),
            Polygon(_, XY) => Arc::new(self.as_polygon().densify(max_distance)),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string().densify(max_distance))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().densify(max_distance)),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
