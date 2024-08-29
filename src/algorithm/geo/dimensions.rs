use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedGeometryArrayTrait};
use crate::datatypes::{Dimension, GeoDataType};
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

impl HasDimensions for &dyn GeometryArrayTrait {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => HasDimensions::is_empty(self.as_point::<2>()),
            GeoDataType::LineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_line_string::<2>())
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_line_string::<2>())
            }
            GeoDataType::Polygon(_, Dimension::XY) => HasDimensions::is_empty(self.as_polygon::<2>()),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_polygon::<2>())
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_multi_point::<2>())
            }
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_multi_point::<2>())
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_multi_line_string::<2>())
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_multi_line_string::<2>())
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_multi_polygon::<2>())
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_multi_polygon::<2>())
            }
            GeoDataType::Mixed(_, Dimension::XY) => HasDimensions::is_empty(self.as_mixed::<2>()),
            GeoDataType::LargeMixed(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_mixed::<2>())
            }
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_geometry_collection::<2>())
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_geometry_collection::<2>())
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

impl HasDimensions for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_empty(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => HasDimensions::is_empty(self.as_point::<2>()),
            GeoDataType::LineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_line_string::<2>())
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_line_string::<2>())
            }
            GeoDataType::Polygon(_, Dimension::XY) => HasDimensions::is_empty(self.as_polygon::<2>()),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_polygon::<2>())
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_multi_point::<2>())
            }
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_multi_point::<2>())
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_multi_line_string::<2>())
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_multi_line_string::<2>())
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_multi_polygon::<2>())
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_multi_polygon::<2>())
            }
            GeoDataType::Mixed(_, Dimension::XY) => HasDimensions::is_empty(self.as_mixed::<2>()),
            GeoDataType::LargeMixed(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_mixed::<2>())
            }
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_geometry_collection::<2>())
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                HasDimensions::is_empty(self.as_large_geometry_collection::<2>())
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
