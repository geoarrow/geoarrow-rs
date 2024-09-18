use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;
use geo::RemoveRepeatedPoints as _RemoveRepeatedPoints;

/// Remove repeated points from a `MultiPoint` and repeated consecutive coordinates
/// from `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon`.
///
/// For `GeometryCollection` it individually removes the repeated points
/// of each geometry in the collection.
///
/// For `Point`, `Line`, `Rect` and `Triangle` the geometry remains the same.
pub trait RemoveRepeatedPoints {
    type Output;

    /// Create a new geometry with (consecutive) repeated points removed.
    fn remove_repeated_points(&self) -> Self::Output;

    // /// Remove (consecutive) repeated points inplace.
    // fn remove_repeated_points_mut(&mut self);
}

// Note: this implementation is outside the macro because it is not generic over O
impl RemoveRepeatedPoints for PointArray<2> {
    type Output = Self;

    fn remove_repeated_points(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl<O: OffsetSizeTrait> RemoveRepeatedPoints for $type {
            type Output = Self;

            fn remove_repeated_points(&self) -> Self::Output {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().for_each(|maybe_g| {
                    output_array
                        .$push_func(maybe_g.map(|geom| geom.remove_repeated_points()).as_ref())
                        .unwrap();
                });

                output_array.finish()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O, 2>, LineStringBuilder<O, 2>, push_line_string);
iter_geo_impl!(PolygonArray<O, 2>, PolygonBuilder<O, 2>, push_polygon);
iter_geo_impl!(MultiPointArray<O, 2>, MultiPointBuilder<O, 2>, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray<O, 2>,
    MultiLineStringBuilder<O, 2>,
    push_multi_line_string
);
iter_geo_impl!(
    MultiPolygonArray<O, 2>,
    MultiPolygonBuilder<O, 2>,
    push_multi_polygon
);
// iter_geo_impl!(MixedGeometryArray<O, 2>, MixedGeometryBuilder<O, 2>, push_geometry);
// iter_geo_impl!(GeometryCollectionArray<O, 2>, geo::GeometryCollection);

impl RemoveRepeatedPoints for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn remove_repeated_points(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().remove_repeated_points()),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().remove_repeated_points()),
            LargeLineString(_, XY) => {
                Arc::new(self.as_large_line_string::<2>().remove_repeated_points())
            }
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().remove_repeated_points()),
            LargePolygon(_, XY) => Arc::new(self.as_large_polygon::<2>().remove_repeated_points()),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().remove_repeated_points()),
            LargeMultiPoint(_, XY) => {
                Arc::new(self.as_large_multi_point::<2>().remove_repeated_points())
            }
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().remove_repeated_points())
            }
            LargeMultiLineString(_, XY) => Arc::new(
                self.as_large_multi_line_string::<2>()
                    .remove_repeated_points(),
            ),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().remove_repeated_points()),
            LargeMultiPolygon(_, XY) => {
                Arc::new(self.as_large_multi_polygon::<2>().remove_repeated_points())
            }
            // Mixed(_, XY) => self.as_mixed::<2>().remove_repeated_points(),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().remove_repeated_points(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().remove_repeated_points(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().remove_repeated_points()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl RemoveRepeatedPoints for ChunkedPointArray<2> {
    type Output = Self;

    fn remove_repeated_points(&self) -> Self::Output {
        self.clone()
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait> RemoveRepeatedPoints for $struct_name {
            type Output = $struct_name;

            fn remove_repeated_points(&self) -> Self::Output {
                self.map(|chunk| chunk.remove_repeated_points())
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<O, 2>);
impl_chunked!(ChunkedPolygonArray<O, 2>);
impl_chunked!(ChunkedMultiPointArray<O, 2>);
impl_chunked!(ChunkedMultiLineStringArray<O, 2>);
impl_chunked!(ChunkedMultiPolygonArray<O, 2>);

impl RemoveRepeatedPoints for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn remove_repeated_points(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().remove_repeated_points()),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().remove_repeated_points()),
            LargeLineString(_, XY) => {
                Arc::new(self.as_large_line_string::<2>().remove_repeated_points())
            }
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().remove_repeated_points()),
            LargePolygon(_, XY) => Arc::new(self.as_large_polygon::<2>().remove_repeated_points()),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().remove_repeated_points()),
            LargeMultiPoint(_, XY) => {
                Arc::new(self.as_large_multi_point::<2>().remove_repeated_points())
            }
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().remove_repeated_points())
            }
            LargeMultiLineString(_, XY) => Arc::new(
                self.as_large_multi_line_string::<2>()
                    .remove_repeated_points(),
            ),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().remove_repeated_points()),
            LargeMultiPolygon(_, XY) => {
                Arc::new(self.as_large_multi_polygon::<2>().remove_repeated_points())
            }
            // Mixed(_, XY) => self.as_mixed::<2>().remove_repeated_points(),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().remove_repeated_points(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().remove_repeated_points(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().remove_repeated_points()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
