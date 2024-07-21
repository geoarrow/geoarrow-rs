use arrow_array::OffsetSizeTrait;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;

/// Computes the total bounds (extent) of the input.
pub trait TotalBounds {
    fn total_bounds(&self) -> BoundingRect;
}

impl TotalBounds for PointArray<2> {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_point(&geom);
        }
        bounds
    }
}

impl TotalBounds for RectArray {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_rect(&geom);
        }
        bounds
    }
}

macro_rules! impl_array {
    ($type:ty, $func:ident) => {
        impl<O: OffsetSizeTrait> TotalBounds for $type {
            fn total_bounds(&self) -> BoundingRect {
                let mut bounds = BoundingRect::new();
                for geom in self.iter().flatten() {
                    bounds.$func(&geom);
                }
                bounds
            }
        }
    };
}

impl_array!(LineStringArray<O, 2>, add_line_string);
impl_array!(PolygonArray<O, 2>, add_polygon);
impl_array!(MultiPointArray<O, 2>, add_multi_point);
impl_array!(MultiLineStringArray<O, 2>, add_multi_line_string);
impl_array!(MultiPolygonArray<O, 2>, add_multi_polygon);
impl_array!(MixedGeometryArray<O, 2>, add_geometry);
impl_array!(GeometryCollectionArray<O, 2>, add_geometry_collection);

impl<O: OffsetSizeTrait> TotalBounds for WKBArray<O> {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_geometry(&geom.to_wkb_object());
        }
        bounds
    }
}

impl TotalBounds for &dyn GeometryArrayTrait {
    fn total_bounds(&self) -> BoundingRect {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point_2d().total_bounds(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string_2d().total_bounds(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string_2d().total_bounds()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon_2d().total_bounds(),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                self.as_large_polygon_2d().total_bounds()
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point_2d().total_bounds(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point_2d().total_bounds()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string_2d().total_bounds()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string_2d().total_bounds()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                self.as_multi_polygon_2d().total_bounds()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon_2d().total_bounds()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed_2d().total_bounds(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().total_bounds(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection_2d().total_bounds()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection_2d().total_bounds()
            }
            GeoDataType::Rect => self.as_rect().total_bounds(),
            GeoDataType::WKB => self.as_wkb().total_bounds(),
            GeoDataType::LargeWKB => self.as_large_wkb().total_bounds(),
            _ => todo!("3d support"),
        }
    }
}

impl<G: GeometryArrayTrait> TotalBounds for ChunkedGeometryArray<G> {
    fn total_bounds(&self) -> BoundingRect {
        let bounding_rects = self.map(|chunk| chunk.as_ref().total_bounds());
        bounding_rects
            .into_iter()
            .fold(BoundingRect::default(), |acc, x| acc + x)
    }
}

impl TotalBounds for &dyn ChunkedGeometryArrayTrait {
    fn total_bounds(&self) -> BoundingRect {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point_2d().total_bounds(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string_2d().total_bounds(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string_2d().total_bounds()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon_2d().total_bounds(),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                self.as_large_polygon_2d().total_bounds()
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point_2d().total_bounds(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point_2d().total_bounds()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string_2d().total_bounds()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string_2d().total_bounds()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                self.as_multi_polygon_2d().total_bounds()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon_2d().total_bounds()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed_2d().total_bounds(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().total_bounds(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection_2d().total_bounds()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection_2d().total_bounds()
            }
            GeoDataType::Rect => self.as_rect().total_bounds(),
            GeoDataType::WKB => self.as_wkb().total_bounds(),
            GeoDataType::LargeWKB => self.as_large_wkb().total_bounds(),
            _ => todo!("3d support"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::test::polygon;

    #[test]
    fn test_dyn_chunked_array() {
        let chunked_array: Arc<dyn ChunkedGeometryArrayTrait> =
            Arc::new(ChunkedGeometryArray::new(vec![
                polygon::p_array(),
                polygon::p_array(),
            ]));
        let total_bounds = chunked_array.as_ref().total_bounds();
        dbg!(total_bounds);
    }

    // #[test]
    // fn test_dyn_chunked_array_dyn_array() {
    //     let dyn_arrs: Vec<Arc<dyn GeometryArrayTrait>> =
    //         vec![Arc::new(polygon::p_array()), Arc::new(polygon::p_array())];
    //     let chunked_array: Arc<dyn ChunkedGeometryArrayTrait> =
    //         Arc::new(ChunkedGeometryArray::new(dyn_arrs));
    //     let total_bounds = chunked_array.as_ref().total_bounds();
    //     dbg!(total_bounds);
    // }
}
