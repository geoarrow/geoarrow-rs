use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::builder::BooleanBuilder;
use arrow_array::BooleanArray;
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

iter_geo_impl!(PointArray);
iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);
iter_geo_impl!(RectArray);
iter_geo_impl!(GeometryArray);

impl HasDimensions for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => HasDimensions::is_empty(self.as_point()),
            LineString(_) => HasDimensions::is_empty(self.as_line_string()),
            Polygon(_) => HasDimensions::is_empty(self.as_polygon()),
            MultiPoint(_) => HasDimensions::is_empty(self.as_multi_point()),
            MultiLineString(_) => HasDimensions::is_empty(self.as_multi_line_string()),
            MultiPolygon(_) => HasDimensions::is_empty(self.as_multi_polygon()),
            GeometryCollection(_) => HasDimensions::is_empty(self.as_geometry_collection()),
            Rect(_) => HasDimensions::is_empty(self.as_rect()),
            Geometry(_) => HasDimensions::is_empty(self.as_geometry()),
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
        use NativeType::*;

        match self.data_type() {
            Point(_) => HasDimensions::is_empty(self.as_point()),
            LineString(_) => HasDimensions::is_empty(self.as_line_string()),
            Polygon(_) => HasDimensions::is_empty(self.as_polygon()),
            MultiPoint(_) => HasDimensions::is_empty(self.as_multi_point()),
            MultiLineString(_) => HasDimensions::is_empty(self.as_multi_line_string()),
            MultiPolygon(_) => HasDimensions::is_empty(self.as_multi_polygon()),
            GeometryCollection(_) => HasDimensions::is_empty(self.as_geometry_collection()),
            Rect(_) => HasDimensions::is_empty(self.as_rect()),
            Geometry(_) => HasDimensions::is_empty(self.as_geometry()),
        }
    }
}
