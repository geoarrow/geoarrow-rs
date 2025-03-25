use crate::array::polygon::PolygonCapacity;
use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPolygonArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::MinimumRotatedRect as _MinimumRotatedRect;

/// Return the minimum bounding rectangle(MBR) of geometry
/// reference: <https://en.wikipedia.org/wiki/Minimum_bounding_box>
/// minimum rotated rect is the rectangle that can enclose all points given
/// and have smallest area of all enclosing rectangles
/// the rect can be any-oriented, not only axis-aligned.
pub trait MinimumRotatedRect {
    type Output;

    fn minimum_rotated_rect(&self) -> Self::Output;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl MinimumRotatedRect for $type {
            type Output = PolygonArray;

            fn minimum_rotated_rect(&self) -> Self::Output {
                // The number of output geoms is the same as the input
                let geom_capacity = self.len();

                // Each output polygon is a simple polygon with only one ring
                let ring_capacity = geom_capacity;

                // Each output polygon has exactly 5 coordinates
                let coord_capacity = ring_capacity * 5;

                let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);

                let mut output_array = PolygonBuilder::with_capacity(Dimension::XY, capacity);

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

iter_geo_impl!(PointArray);
iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);

impl MinimumRotatedRect for &dyn NativeArray {
    type Output = Result<PolygonArray>;

    fn minimum_rotated_rect(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point().minimum_rotated_rect(),
            LineString(_, XY) => self.as_line_string().minimum_rotated_rect(),
            Polygon(_, XY) => self.as_polygon().minimum_rotated_rect(),
            MultiPoint(_, XY) => self.as_multi_point().minimum_rotated_rect(),
            MultiLineString(_, XY) => self.as_multi_line_string().minimum_rotated_rect(),
            MultiPolygon(_, XY) => self.as_multi_polygon().minimum_rotated_rect(),
            GeometryCollection(_, XY) => self.as_geometry_collection().minimum_rotated_rect(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> MinimumRotatedRect for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<PolygonArray>>;

    fn minimum_rotated_rect(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().minimum_rotated_rect())?
            .try_into()
    }
}

impl MinimumRotatedRect for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPolygonArray>;

    fn minimum_rotated_rect(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point().minimum_rotated_rect(),
            LineString(_, XY) => self.as_line_string().minimum_rotated_rect(),
            Polygon(_, XY) => self.as_polygon().minimum_rotated_rect(),
            MultiPoint(_, XY) => self.as_multi_point().minimum_rotated_rect(),
            MultiLineString(_, XY) => self.as_multi_line_string().minimum_rotated_rect(),
            MultiPolygon(_, XY) => self.as_multi_polygon().minimum_rotated_rect(),
            GeometryCollection(_, XY) => self.as_geometry_collection().minimum_rotated_rect(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
