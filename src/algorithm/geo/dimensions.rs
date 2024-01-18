use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
use geo::dimensions::HasDimensions as GeoHasDimensions;

/// Operate on the dimensionality of geometries.
pub trait HasDimensions {
    type Output;

    /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these `empty`.
    ///
    /// Types like `Point` and `Rect`, which have at least one coordinate by construction, can
    /// never be considered empty.
    /// ```
    /// use geo::{Point, coord, LineString};
    /// use geo::HasDimensions;
    ///
    /// let line_string = LineString::new(vec![
    ///     coord! { x: 0., y: 0. },
    ///     coord! { x: 10., y: 0. },
    /// ]);
    /// assert!(!line_string.is_empty());
    ///
    /// let empty_line_string: LineString = LineString::new(vec![]);
    /// assert!(empty_line_string.is_empty());
    ///
    /// let point = Point::new(0.0, 0.0);
    /// assert!(!point.is_empty());
    /// ```
    fn is_empty(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl HasDimensions for PointArray {
    type Output = BooleanArray;

    fn is_empty(&self) -> Self::Output {
        let mut output_array = BooleanBuilder::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.append_option(maybe_g.map(|g| g.is_empty())));
        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> HasDimensions for $type {
            type Output = BooleanArray;

            fn is_empty(&self) -> Self::Output {
                let mut output_array = BooleanBuilder::with_capacity(self.len());
                self.iter_geo()
                    .for_each(|maybe_g| output_array.append_option(maybe_g.map(|g| g.is_empty())));
                output_array.finish()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(MixedGeometryArray<O>);
iter_geo_impl!(GeometryCollectionArray<O>);
iter_geo_impl!(WKBArray<O>);

impl HasDimensions for &dyn GeometryArrayTrait {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => HasDimensions::is_empty(self.as_point()),
            GeoDataType::LineString(_) => HasDimensions::is_empty(self.as_line_string()),
            GeoDataType::LargeLineString(_) => HasDimensions::is_empty(self.as_large_line_string()),
            GeoDataType::Polygon(_) => HasDimensions::is_empty(self.as_polygon()),
            GeoDataType::LargePolygon(_) => HasDimensions::is_empty(self.as_large_polygon()),
            GeoDataType::MultiPoint(_) => HasDimensions::is_empty(self.as_multi_point()),
            GeoDataType::LargeMultiPoint(_) => HasDimensions::is_empty(self.as_large_multi_point()),
            GeoDataType::MultiLineString(_) => HasDimensions::is_empty(self.as_multi_line_string()),
            GeoDataType::LargeMultiLineString(_) => {
                HasDimensions::is_empty(self.as_large_multi_line_string())
            }
            GeoDataType::MultiPolygon(_) => HasDimensions::is_empty(self.as_multi_polygon()),
            GeoDataType::LargeMultiPolygon(_) => {
                HasDimensions::is_empty(self.as_large_multi_polygon())
            }
            GeoDataType::Mixed(_) => HasDimensions::is_empty(self.as_mixed()),
            GeoDataType::LargeMixed(_) => HasDimensions::is_empty(self.as_large_mixed()),
            GeoDataType::GeometryCollection(_) => {
                HasDimensions::is_empty(self.as_geometry_collection())
            }
            GeoDataType::LargeGeometryCollection(_) => {
                HasDimensions::is_empty(self.as_large_geometry_collection())
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> HasDimensions for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_empty(&self) -> Self::Output {
        self.try_map(|chunk| HasDimensions::is_empty(&chunk.as_ref()))?
            .try_into()
    }
}
