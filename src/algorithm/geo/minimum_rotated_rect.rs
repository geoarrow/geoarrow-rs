use crate::array::polygon::PolygonCapacity;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::MinimumRotatedRect as _MinimumRotatedRect;

/// Return the minimum bounding rectangle(MBR) of geometry
/// reference: <https://en.wikipedia.org/wiki/Minimum_bounding_box>
/// minimum rotated rect is the rectangle that can enclose all points given
/// and have smallest area of all enclosing rectangles
/// the rect can be any-oriented, not only axis-aligned.
///
/// # Examples
///
/// ```
/// use geo::{line_string, polygon, LineString, Polygon};
/// use geo::MinimumRotatedRect;
/// let poly: Polygon<f64> = polygon![(x: 3.3, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)];
/// let mbr = MinimumRotatedRect::minimum_rotated_rect(&poly).unwrap();
/// assert_eq!(
///     mbr.exterior(),
///     &LineString::from(vec![
///         (1.7000000000000006, 24.6),
///         (1.4501458363715918, 30.446587428904767),
///         (14.4, 31.0),
///         (14.649854163628408, 25.153412571095235),
///         (1.7000000000000006, 24.6),
///     ])
/// );
/// ```
pub trait MinimumRotatedRect<O: OffsetSizeTrait> {
    fn minimum_rotated_rect(&self) -> PolygonArray<O>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl MinimumRotatedRect<i32> for PointArray {
    fn minimum_rotated_rect(&self) -> PolygonArray<i32> {
        // The number of output geoms is the same as the input
        let geom_capacity = self.len();

        // Each output polygon is a simple polygon with only one ring
        let ring_capacity = geom_capacity;

        // Each output polygon has exactly 5 coordinates
        let coord_capacity = ring_capacity * 5;

        let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);

        let mut output_array = PolygonBuilder::with_capacity(capacity);

        self.iter_geo().for_each(|maybe_g| {
            output_array
                .push_polygon(maybe_g.and_then(|g| g.minimum_rotated_rect()).as_ref())
                .unwrap()
        });

        output_array.into()
    }
}

impl MinimumRotatedRect<i64> for PointArray {
    fn minimum_rotated_rect(&self) -> PolygonArray<i64> {
        // The number of output geoms is the same as the input
        let geom_capacity = self.len();

        // Each output polygon is a simple polygon with only one ring
        let ring_capacity = geom_capacity;

        // Each output polygon has exactly 5 coordinates
        let coord_capacity = ring_capacity * 5;

        let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);

        let mut output_array = PolygonBuilder::with_capacity(capacity);

        self.iter_geo().for_each(|maybe_g| {
            output_array
                .push_polygon(maybe_g.and_then(|g| g.minimum_rotated_rect()).as_ref())
                .unwrap()
        });

        output_array.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $offset_type:ty) => {
        impl<O: OffsetSizeTrait> MinimumRotatedRect<$offset_type> for $type {
            fn minimum_rotated_rect(&self) -> PolygonArray<$offset_type> {
                // The number of output geoms is the same as the input
                let geom_capacity = self.len();

                // Each output polygon is a simple polygon with only one ring
                let ring_capacity = geom_capacity;

                // Each output polygon has exactly 5 coordinates
                let coord_capacity = ring_capacity * 5;

                let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);

                let mut output_array = PolygonBuilder::with_capacity(capacity);

                self.iter_geo().for_each(|maybe_g| {
                    output_array
                        .push_polygon(maybe_g.and_then(|g| g.minimum_rotated_rect()).as_ref())
                        .unwrap()
                });

                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>, i32);
iter_geo_impl!(LineStringArray<O>, i64);

iter_geo_impl!(PolygonArray<O>, i32);
iter_geo_impl!(PolygonArray<O>, i64);

iter_geo_impl!(MultiPointArray<O>, i32);
iter_geo_impl!(MultiPointArray<O>, i64);

iter_geo_impl!(MultiLineStringArray<O>, i32);
iter_geo_impl!(MultiLineStringArray<O>, i64);

iter_geo_impl!(MultiPolygonArray<O>, i32);
iter_geo_impl!(MultiPolygonArray<O>, i64);

impl<O: OffsetSizeTrait> MinimumRotatedRect<i32> for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn minimum_rotated_rect(&self) -> PolygonArray<i32>;
    }
}

// impl<O: OffsetSizeTrait> MinimumRotatedRect<i64> for GeometryArray<O> {
//     crate::geometry_array_delegate_impl! {
//         fn minimum_rotated_rect(&self) -> PolygonArray<i64>;
//     }
// }
