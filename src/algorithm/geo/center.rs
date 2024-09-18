use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPointArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;
use geo::BoundingRect;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of that box
pub trait Center {
    type Output;

    fn center(&self) -> Self::Output;
}

impl Center for PointArray<2> {
    type Output = PointArray<2>;

    fn center(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Center for $type {
            type Output = PointArray<2>;

            fn center(&self) -> Self::Output {
                let mut output_array = PointBuilder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_point(
                        maybe_g
                            .and_then(|g| g.bounding_rect().map(|rect| rect.center()))
                            .as_ref(),
                    )
                });
                output_array.into()
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

impl Center for &dyn NativeArray {
    type Output = Result<PointArray<2>>;

    fn center(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().center(),
            LineString(_, XY) => self.as_line_string::<2>().center(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().center(),
            Polygon(_, XY) => self.as_polygon::<2>().center(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().center(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().center(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().center(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().center(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().center(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().center(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().center(),
            Mixed(_, XY) => self.as_mixed::<2>().center(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().center(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().center(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().center(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> Center for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedPointArray<2>>;

    fn center(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().center())?.try_into()
    }
}

impl Center for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPointArray<2>>;

    fn center(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().center(),
            LineString(_, XY) => self.as_line_string::<2>().center(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().center(),
            Polygon(_, XY) => self.as_polygon::<2>().center(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().center(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().center(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().center(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().center(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().center(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().center(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().center(),
            Mixed(_, XY) => self.as_mixed::<2>().center(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().center(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().center(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().center(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
