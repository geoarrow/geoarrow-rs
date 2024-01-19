use std::sync::Arc;

use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
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
impl RemoveRepeatedPoints for PointArray {
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

iter_geo_impl!(LineStringArray<O>, LineStringBuilder<O>, push_line_string);
iter_geo_impl!(PolygonArray<O>, PolygonBuilder<O>, push_polygon);
iter_geo_impl!(MultiPointArray<O>, MultiPointBuilder<O>, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray<O>,
    MultiLineStringBuilder<O>,
    push_multi_line_string
);
iter_geo_impl!(
    MultiPolygonArray<O>,
    MultiPolygonBuilder<O>,
    push_multi_polygon
);
// iter_geo_impl!(MixedGeometryArray<O>, MixedGeometryBuilder<O>, push_geometry);
// iter_geo_impl!(GeometryCollectionArray<O>, geo::GeometryCollection);

impl RemoveRepeatedPoints for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn remove_repeated_points(&self) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().remove_repeated_points()),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().remove_repeated_points()),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().remove_repeated_points())
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().remove_repeated_points()),
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().remove_repeated_points())
            }
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().remove_repeated_points()),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().remove_repeated_points())
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().remove_repeated_points())
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().remove_repeated_points())
            }
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().remove_repeated_points())
            }
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().remove_repeated_points())
            }
            // GeoDataType::Mixed(_) => self.as_mixed().remove_repeated_points(),
            // GeoDataType::LargeMixed(_) => self.as_large_mixed().remove_repeated_points(),
            // GeoDataType::GeometryCollection(_) => self.as_geometry_collection().remove_repeated_points(),
            // GeoDataType::LargeGeometryCollection(_) => {
            //     self.as_large_geometry_collection().remove_repeated_points()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
