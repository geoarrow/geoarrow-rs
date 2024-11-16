use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPointArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::BoundingRect;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of that box
pub trait Center {
    type Output;

    fn center(&self) -> Self::Output;
}

impl Center for PointArray {
    type Output = PointArray;

    fn center(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl Center for $type {
            type Output = PointArray;

            fn center(&self) -> Self::Output {
                let mut output_array = PointBuilder::with_capacity(Dimension::XY, self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_coord(
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

iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);

impl Center for &dyn NativeArray {
    type Output = Result<PointArray>;

    fn center(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point().center(),
            LineString(_, XY) => self.as_line_string().center(),
            Polygon(_, XY) => self.as_polygon().center(),
            MultiPoint(_, XY) => self.as_multi_point().center(),
            MultiLineString(_, XY) => self.as_multi_line_string().center(),
            MultiPolygon(_, XY) => self.as_multi_polygon().center(),
            Mixed(_, XY) => self.as_mixed().center(),
            GeometryCollection(_, XY) => self.as_geometry_collection().center(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> Center for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedPointArray>;

    fn center(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().center())?.try_into()
    }
}

impl Center for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPointArray>;

    fn center(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point().center(),
            LineString(_, XY) => self.as_line_string().center(),
            Polygon(_, XY) => self.as_polygon().center(),
            MultiPoint(_, XY) => self.as_multi_point().center(),
            MultiLineString(_, XY) => self.as_multi_line_string().center(),
            MultiPolygon(_, XY) => self.as_multi_polygon().center(),
            Mixed(_, XY) => self.as_mixed().center(),
            GeometryCollection(_, XY) => self.as_geometry_collection().center(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
