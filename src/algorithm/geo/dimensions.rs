use crate::array::*;
use crate::GeometryArrayTrait;
use arrow2::array::{BooleanArray, MutableBooleanArray};
use geo::dimensions::HasDimensions as GeoHasDimensions;

/// Operate on the dimensionality of geometries.
pub trait HasDimensions {
    /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these `empty`.
    ///
    /// Types like `Point` and `Rect`, which have at least one coordinate by construction, can
    /// never be considered empty.
    /// ```
    /// use geo_types::{Point, coord, LineString};
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
    fn is_empty(&self) -> BooleanArray;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ident) => {
        impl HasDimensions for $type {
            fn is_empty(&self) -> BooleanArray {
                let mut output_array = MutableBooleanArray::with_capacity(self.len());
                self.iter_geo()
                    .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
                output_array.into()
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
iter_geo_impl!(WKBArray);

impl HasDimensions for GeometryArray {
    fn is_empty(&self) -> BooleanArray {
        match self {
            GeometryArray::WKB(arr) => HasDimensions::is_empty(arr),
            GeometryArray::Point(arr) => HasDimensions::is_empty(arr),
            GeometryArray::LineString(arr) => HasDimensions::is_empty(arr),
            GeometryArray::Polygon(arr) => HasDimensions::is_empty(arr),
            GeometryArray::MultiPoint(arr) => HasDimensions::is_empty(arr),
            GeometryArray::MultiLineString(arr) => HasDimensions::is_empty(arr),
            GeometryArray::MultiPolygon(arr) => HasDimensions::is_empty(arr),
        }
    }
}
