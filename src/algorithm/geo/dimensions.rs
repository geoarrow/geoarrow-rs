use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
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
        impl HasDimensions for $type {
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

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);

impl HasDimensions for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => HasDimensions::is_empty(self.as_point::<2>()),
            LineString(_, XY) => HasDimensions::is_empty(self.as_line_string::<2>()),
            Polygon(_, XY) => HasDimensions::is_empty(self.as_polygon::<2>()),
            MultiPoint(_, XY) => HasDimensions::is_empty(self.as_multi_point::<2>()),
            MultiLineString(_, XY) => HasDimensions::is_empty(self.as_multi_line_string::<2>()),
            MultiPolygon(_, XY) => HasDimensions::is_empty(self.as_multi_polygon::<2>()),
            Mixed(_, XY) => HasDimensions::is_empty(self.as_mixed::<2>()),
            GeometryCollection(_, XY) => {
                HasDimensions::is_empty(self.as_geometry_collection::<2>())
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
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => HasDimensions::is_empty(self.as_point::<2>()),
            LineString(_, XY) => HasDimensions::is_empty(self.as_line_string::<2>()),
            Polygon(_, XY) => HasDimensions::is_empty(self.as_polygon::<2>()),
            MultiPoint(_, XY) => HasDimensions::is_empty(self.as_multi_point::<2>()),
            MultiLineString(_, XY) => HasDimensions::is_empty(self.as_multi_line_string::<2>()),
            MultiPolygon(_, XY) => HasDimensions::is_empty(self.as_multi_polygon::<2>()),
            Mixed(_, XY) => HasDimensions::is_empty(self.as_mixed::<2>()),
            GeometryCollection(_, XY) => {
                HasDimensions::is_empty(self.as_geometry_collection::<2>())
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
