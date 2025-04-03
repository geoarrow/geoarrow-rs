use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
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
impl RemoveRepeatedPoints for PointArray {
    type Output = Self;

    fn remove_repeated_points(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl RemoveRepeatedPoints for $type {
            type Output = Self;

            fn remove_repeated_points(&self) -> Self::Output {
                let mut output_array =
                    <$builder_type>::with_capacity(Dimension::XY, self.buffer_lengths());

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

iter_geo_impl!(LineStringArray, LineStringBuilder, push_line_string);
iter_geo_impl!(PolygonArray, PolygonBuilder, push_polygon);
iter_geo_impl!(MultiPointArray, MultiPointBuilder, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray,
    MultiLineStringBuilder,
    push_multi_line_string
);
iter_geo_impl!(MultiPolygonArray, MultiPolygonBuilder, push_multi_polygon);
// iter_geo_impl!(MixedGeometryArray, MixedGeometryBuilder, push_geometry);
// iter_geo_impl!(GeometryCollectionArray, geo::GeometryCollection);

impl RemoveRepeatedPoints for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn remove_repeated_points(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_) => Arc::new(self.as_point().remove_repeated_points()),
            LineString(_) => Arc::new(self.as_line_string().remove_repeated_points()),
            Polygon(_) => Arc::new(self.as_polygon().remove_repeated_points()),
            MultiPoint(_) => Arc::new(self.as_multi_point().remove_repeated_points()),
            MultiLineString(_) => Arc::new(self.as_multi_line_string().remove_repeated_points()),
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().remove_repeated_points()),
            // Mixed(_) => self.as_mixed().remove_repeated_points(),
            // GeometryCollection(_) => self.as_geometry_collection().remove_repeated_points(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl RemoveRepeatedPoints for ChunkedPointArray {
    type Output = Self;

    fn remove_repeated_points(&self) -> Self::Output {
        self.clone()
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl RemoveRepeatedPoints for $struct_name {
            type Output = $struct_name;

            fn remove_repeated_points(&self) -> Self::Output {
                self.map(|chunk| chunk.remove_repeated_points())
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);

impl RemoveRepeatedPoints for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn remove_repeated_points(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_) => Arc::new(self.as_point().remove_repeated_points()),
            LineString(_) => Arc::new(self.as_line_string().remove_repeated_points()),
            Polygon(_) => Arc::new(self.as_polygon().remove_repeated_points()),
            MultiPoint(_) => Arc::new(self.as_multi_point().remove_repeated_points()),
            MultiLineString(_) => Arc::new(self.as_multi_line_string().remove_repeated_points()),
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().remove_repeated_points()),
            // Mixed(_) => self.as_mixed().remove_repeated_points(),
            // GeometryCollection(_) => self.as_geometry_collection().remove_repeated_points(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
