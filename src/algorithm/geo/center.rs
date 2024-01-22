use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
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

impl Center for PointArray {
    type Output = PointArray;

    fn center(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Center for $type {
            type Output = PointArray;

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

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(MixedGeometryArray<O>);
iter_geo_impl!(GeometryCollectionArray<O>);
iter_geo_impl!(WKBArray<O>);

impl Center for &dyn GeometryArrayTrait {
    type Output = Result<PointArray>;

    fn center(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().center(),
            GeoDataType::LineString(_) => self.as_line_string().center(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().center(),
            GeoDataType::Polygon(_) => self.as_polygon().center(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().center(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().center(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().center(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().center(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().center(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().center(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().center(),
            GeoDataType::Mixed(_) => self.as_mixed().center(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().center(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().center(),
            GeoDataType::LargeGeometryCollection(_) => self.as_large_geometry_collection().center(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> Center for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<PointArray>>;

    fn center(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().center())?.try_into()
    }
}
