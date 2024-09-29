use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPointArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
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
        impl Center for $type {
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

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);

impl Center for &dyn NativeArray {
    type Output = Result<PointArray<2>>;

    fn center(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().center(),
            LineString(_, XY) => self.as_line_string::<2>().center(),
            Polygon(_, XY) => self.as_polygon::<2>().center(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().center(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().center(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().center(),
            Mixed(_, XY) => self.as_mixed::<2>().center(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().center(),
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
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().center(),
            LineString(_, XY) => self.as_line_string::<2>().center(),
            Polygon(_, XY) => self.as_polygon::<2>().center(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().center(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().center(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().center(),
            Mixed(_, XY) => self.as_mixed::<2>().center(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().center(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
