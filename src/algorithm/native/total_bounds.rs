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

impl<const D: usize> TotalBounds for PointArray<D> {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for geom in self.iter().flatten() {
            bounds.add_point(&geom);
        }
        bounds
    }
}

impl<const D: usize> TotalBounds for RectArray<D> {
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
        impl<O: OffsetSizeTrait, const D: usize> TotalBounds for $type {
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

impl_array!(LineStringArray<O, D>, add_line_string);
impl_array!(PolygonArray<O, D>, add_polygon);
impl_array!(MultiPointArray<O, D>, add_multi_point);
impl_array!(MultiLineStringArray<O, D>, add_multi_line_string);
impl_array!(MultiPolygonArray<O, D>, add_multi_polygon);
impl_array!(MixedGeometryArray<O, D>, add_geometry);
impl_array!(GeometryCollectionArray<O, D>, add_geometry_collection);

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
            GeoDataType::Point(_, Dimension::XY) => self.as_point::<2>().total_bounds(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string::<2>().total_bounds(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string::<2>().total_bounds()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon::<2>().total_bounds(),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                self.as_large_polygon::<2>().total_bounds()
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point::<2>().total_bounds(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point::<2>().total_bounds()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string::<2>().total_bounds()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string::<2>().total_bounds()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                self.as_multi_polygon::<2>().total_bounds()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon::<2>().total_bounds()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed::<2>().total_bounds(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed::<2>().total_bounds(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection::<2>().total_bounds()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection::<2>().total_bounds()
            }
            GeoDataType::Rect(Dimension::XY) => self.as_rect::<2>().total_bounds(),
            GeoDataType::Point(_, Dimension::XYZ) => self.as_point::<3>().total_bounds(),
            GeoDataType::LineString(_, Dimension::XYZ) => self.as_line_string::<3>().total_bounds(),
            GeoDataType::LargeLineString(_, Dimension::XYZ) => {
                self.as_large_line_string::<3>().total_bounds()
            }
            GeoDataType::Polygon(_, Dimension::XYZ) => self.as_polygon::<3>().total_bounds(),
            GeoDataType::LargePolygon(_, Dimension::XYZ) => {
                self.as_large_polygon::<3>().total_bounds()
            }
            GeoDataType::MultiPoint(_, Dimension::XYZ) => self.as_multi_point::<3>().total_bounds(),
            GeoDataType::LargeMultiPoint(_, Dimension::XYZ) => {
                self.as_large_multi_point::<3>().total_bounds()
            }
            GeoDataType::MultiLineString(_, Dimension::XYZ) => {
                self.as_multi_line_string::<3>().total_bounds()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XYZ) => {
                self.as_large_multi_line_string::<3>().total_bounds()
            }
            GeoDataType::MultiPolygon(_, Dimension::XYZ) => {
                self.as_multi_polygon::<3>().total_bounds()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XYZ) => {
                self.as_large_multi_polygon::<3>().total_bounds()
            }
            GeoDataType::Mixed(_, Dimension::XYZ) => self.as_mixed::<3>().total_bounds(),
            GeoDataType::LargeMixed(_, Dimension::XYZ) => self.as_large_mixed::<3>().total_bounds(),
            GeoDataType::GeometryCollection(_, Dimension::XYZ) => {
                self.as_geometry_collection::<3>().total_bounds()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XYZ) => {
                self.as_large_geometry_collection::<3>().total_bounds()
            }
            GeoDataType::Rect(Dimension::XYZ) => self.as_rect::<3>().total_bounds(),
            GeoDataType::WKB => self.as_wkb().total_bounds(),
            GeoDataType::LargeWKB => self.as_large_wkb().total_bounds(),
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
            GeoDataType::Point(_, Dimension::XY) => self.as_point::<2>().total_bounds(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string::<2>().total_bounds(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string::<2>().total_bounds()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon::<2>().total_bounds(),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                self.as_large_polygon::<2>().total_bounds()
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point::<2>().total_bounds(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point::<2>().total_bounds()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string::<2>().total_bounds()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string::<2>().total_bounds()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                self.as_multi_polygon::<2>().total_bounds()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon::<2>().total_bounds()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed::<2>().total_bounds(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed::<2>().total_bounds(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection::<2>().total_bounds()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection::<2>().total_bounds()
            }
            GeoDataType::Rect(Dimension::XY) => self.as_rect::<2>().total_bounds(),
            GeoDataType::Point(_, Dimension::XYZ) => self.as_point::<3>().total_bounds(),
            GeoDataType::LineString(_, Dimension::XYZ) => self.as_line_string::<3>().total_bounds(),
            GeoDataType::LargeLineString(_, Dimension::XYZ) => {
                self.as_large_line_string::<3>().total_bounds()
            }
            GeoDataType::Polygon(_, Dimension::XYZ) => self.as_polygon::<3>().total_bounds(),
            GeoDataType::LargePolygon(_, Dimension::XYZ) => {
                self.as_large_polygon::<3>().total_bounds()
            }
            GeoDataType::MultiPoint(_, Dimension::XYZ) => self.as_multi_point::<3>().total_bounds(),
            GeoDataType::LargeMultiPoint(_, Dimension::XYZ) => {
                self.as_large_multi_point::<3>().total_bounds()
            }
            GeoDataType::MultiLineString(_, Dimension::XYZ) => {
                self.as_multi_line_string::<3>().total_bounds()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XYZ) => {
                self.as_large_multi_line_string::<3>().total_bounds()
            }
            GeoDataType::MultiPolygon(_, Dimension::XYZ) => {
                self.as_multi_polygon::<3>().total_bounds()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XYZ) => {
                self.as_large_multi_polygon::<3>().total_bounds()
            }
            GeoDataType::Mixed(_, Dimension::XYZ) => self.as_mixed::<3>().total_bounds(),
            GeoDataType::LargeMixed(_, Dimension::XYZ) => self.as_large_mixed::<3>().total_bounds(),
            GeoDataType::GeometryCollection(_, Dimension::XYZ) => {
                self.as_geometry_collection::<3>().total_bounds()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XYZ) => {
                self.as_large_geometry_collection::<3>().total_bounds()
            }
            GeoDataType::Rect(Dimension::XYZ) => self.as_rect::<3>().total_bounds(),
            GeoDataType::WKB => self.as_wkb().total_bounds(),
            GeoDataType::LargeWKB => self.as_large_wkb().total_bounds(),
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
