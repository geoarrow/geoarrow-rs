use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeArrayAccessor;
use crate::NativeArray;
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
impl HasDimensions for PointArray<2> {
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

iter_geo_impl!(LineStringArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>);
iter_geo_impl!(MixedGeometryArray<O, 2>);
iter_geo_impl!(GeometryCollectionArray<O, 2>);
iter_geo_impl!(WKBArray<O>);

impl HasDimensions for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            Point(_, XY) => HasDimensions::is_empty(self.as_point::<2>()),
            LineString(_, XY) => HasDimensions::is_empty(self.as_line_string::<2>()),
            LargeLineString(_, XY) => HasDimensions::is_empty(self.as_large_line_string::<2>()),
            Polygon(_, XY) => HasDimensions::is_empty(self.as_polygon::<2>()),
            LargePolygon(_, XY) => HasDimensions::is_empty(self.as_large_polygon::<2>()),
            MultiPoint(_, XY) => HasDimensions::is_empty(self.as_multi_point::<2>()),
            LargeMultiPoint(_, XY) => HasDimensions::is_empty(self.as_large_multi_point::<2>()),
            MultiLineString(_, XY) => HasDimensions::is_empty(self.as_multi_line_string::<2>()),
            LargeMultiLineString(_, XY) => {
                HasDimensions::is_empty(self.as_large_multi_line_string::<2>())
            }
            MultiPolygon(_, XY) => HasDimensions::is_empty(self.as_multi_polygon::<2>()),
            LargeMultiPolygon(_, XY) => HasDimensions::is_empty(self.as_large_multi_polygon::<2>()),
            Mixed(_, XY) => HasDimensions::is_empty(self.as_mixed::<2>()),
            LargeMixed(_, XY) => HasDimensions::is_empty(self.as_large_mixed::<2>()),
            GeometryCollection(_, XY) => {
                HasDimensions::is_empty(self.as_geometry_collection::<2>())
            }
            LargeGeometryCollection(_, XY) => {
                HasDimensions::is_empty(self.as_large_geometry_collection::<2>())
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> HasDimensions for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_empty(&self) -> Self::Output {
        self.try_map(|chunk| HasDimensions::is_empty(&chunk.as_ref()))?
            .try_into()
    }
}

impl HasDimensions for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_empty(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => HasDimensions::is_empty(self.as_point::<2>()),
            LineString(_, XY) => HasDimensions::is_empty(self.as_line_string::<2>()),
            LargeLineString(_, XY) => HasDimensions::is_empty(self.as_large_line_string::<2>()),
            Polygon(_, XY) => HasDimensions::is_empty(self.as_polygon::<2>()),
            LargePolygon(_, XY) => HasDimensions::is_empty(self.as_large_polygon::<2>()),
            MultiPoint(_, XY) => HasDimensions::is_empty(self.as_multi_point::<2>()),
            LargeMultiPoint(_, XY) => HasDimensions::is_empty(self.as_large_multi_point::<2>()),
            MultiLineString(_, XY) => HasDimensions::is_empty(self.as_multi_line_string::<2>()),
            LargeMultiLineString(_, XY) => {
                HasDimensions::is_empty(self.as_large_multi_line_string::<2>())
            }
            MultiPolygon(_, XY) => HasDimensions::is_empty(self.as_multi_polygon::<2>()),
            LargeMultiPolygon(_, XY) => HasDimensions::is_empty(self.as_large_multi_polygon::<2>()),
            Mixed(_, XY) => HasDimensions::is_empty(self.as_mixed::<2>()),
            LargeMixed(_, XY) => HasDimensions::is_empty(self.as_large_mixed::<2>()),
            GeometryCollection(_, XY) => {
                HasDimensions::is_empty(self.as_geometry_collection::<2>())
            }
            LargeGeometryCollection(_, XY) => {
                HasDimensions::is_empty(self.as_large_geometry_collection::<2>())
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
