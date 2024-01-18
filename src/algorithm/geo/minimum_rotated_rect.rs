use crate::array::polygon::PolygonCapacity;
use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
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
    type Output;

    fn minimum_rotated_rect(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl<O: OffsetSizeTrait> MinimumRotatedRect<O> for PointArray {
    type Output = PolygonArray<O>;

    fn minimum_rotated_rect(&self) -> Self::Output {
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
    ($type:ty) => {
        impl<OOutput: OffsetSizeTrait, OInput: OffsetSizeTrait> MinimumRotatedRect<OOutput>
            for $type
        {
            type Output = PolygonArray<OOutput>;

            fn minimum_rotated_rect(&self) -> Self::Output {
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

iter_geo_impl!(LineStringArray<OInput>);
iter_geo_impl!(PolygonArray<OInput>);
iter_geo_impl!(MultiPointArray<OInput>);
iter_geo_impl!(MultiLineStringArray<OInput>);
iter_geo_impl!(MultiPolygonArray<OInput>);
iter_geo_impl!(MixedGeometryArray<OInput>);
iter_geo_impl!(GeometryCollectionArray<OInput>);

impl<O: OffsetSizeTrait> MinimumRotatedRect<O> for &dyn GeometryArrayTrait {
    type Output = Result<PolygonArray<O>>;

    fn minimum_rotated_rect(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().minimum_rotated_rect(),
            GeoDataType::LineString(_) => self.as_line_string().minimum_rotated_rect(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().minimum_rotated_rect(),
            GeoDataType::Polygon(_) => self.as_polygon().minimum_rotated_rect(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().minimum_rotated_rect(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().minimum_rotated_rect(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().minimum_rotated_rect(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().minimum_rotated_rect(),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().minimum_rotated_rect()
            }
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().minimum_rotated_rect(),
            GeoDataType::LargeMultiPolygon(_) => {
                self.as_large_multi_polygon().minimum_rotated_rect()
            }
            GeoDataType::Mixed(_) => self.as_mixed().minimum_rotated_rect(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().minimum_rotated_rect(),
            GeoDataType::GeometryCollection(_) => {
                self.as_geometry_collection().minimum_rotated_rect()
            }
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().minimum_rotated_rect()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<O: OffsetSizeTrait, G: GeometryArrayTrait> MinimumRotatedRect<O> for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<PolygonArray<O>>>;

    fn minimum_rotated_rect(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().minimum_rotated_rect())?
            .try_into()
    }
}
