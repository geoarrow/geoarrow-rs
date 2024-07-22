use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedGeometryArrayTrait, ChunkedPointArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
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

impl Center for &dyn GeometryArrayTrait {
    type Output = Result<PointArray<2>>;

    fn center(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point_2d().center(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string_2d().center(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string_2d().center()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon_2d().center(),
            GeoDataType::LargePolygon(_, Dimension::XY) => self.as_large_polygon_2d().center(),
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point_2d().center(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point_2d().center()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string_2d().center()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string_2d().center()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => self.as_multi_polygon_2d().center(),
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon_2d().center()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed_2d().center(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().center(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection_2d().center()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection_2d().center()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> Center for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedPointArray<2>>;

    fn center(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().center())?.try_into()
    }
}

impl Center for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedPointArray<2>>;

    fn center(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point_2d().center(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string_2d().center(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string_2d().center()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon_2d().center(),
            GeoDataType::LargePolygon(_, Dimension::XY) => self.as_large_polygon_2d().center(),
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point_2d().center(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point_2d().center()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string_2d().center()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string_2d().center()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => self.as_multi_polygon_2d().center(),
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon_2d().center()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed_2d().center(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().center(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection_2d().center()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection_2d().center()
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
