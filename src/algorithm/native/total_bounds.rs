use arrow_array::OffsetSizeTrait;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::trait_::NativeArrayAccessor;
use crate::NativeArray;

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

impl TotalBounds for &dyn NativeArray {
    fn total_bounds(&self) -> BoundingRect {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().total_bounds(),
            LineString(_, XY) => self.as_line_string::<2>().total_bounds(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().total_bounds(),
            Polygon(_, XY) => self.as_polygon::<2>().total_bounds(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().total_bounds(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().total_bounds(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().total_bounds(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().total_bounds(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().total_bounds(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().total_bounds(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().total_bounds(),
            Mixed(_, XY) => self.as_mixed::<2>().total_bounds(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().total_bounds(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().total_bounds(),
            LargeGeometryCollection(_, XY) => {
                self.as_large_geometry_collection::<2>().total_bounds()
            }
            Rect(XY) => self.as_rect::<2>().total_bounds(),
            Point(_, XYZ) => self.as_point::<3>().total_bounds(),
            LineString(_, XYZ) => self.as_line_string::<3>().total_bounds(),
            LargeLineString(_, XYZ) => self.as_large_line_string::<3>().total_bounds(),
            Polygon(_, XYZ) => self.as_polygon::<3>().total_bounds(),
            LargePolygon(_, XYZ) => self.as_large_polygon::<3>().total_bounds(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().total_bounds(),
            LargeMultiPoint(_, XYZ) => self.as_large_multi_point::<3>().total_bounds(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().total_bounds(),
            LargeMultiLineString(_, XYZ) => self.as_large_multi_line_string::<3>().total_bounds(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().total_bounds(),
            LargeMultiPolygon(_, XYZ) => self.as_large_multi_polygon::<3>().total_bounds(),
            Mixed(_, XYZ) => self.as_mixed::<3>().total_bounds(),
            LargeMixed(_, XYZ) => self.as_large_mixed::<3>().total_bounds(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().total_bounds(),
            LargeGeometryCollection(_, XYZ) => {
                self.as_large_geometry_collection::<3>().total_bounds()
            }
            Rect(XYZ) => self.as_rect::<3>().total_bounds(),
            WKB => self.as_wkb().total_bounds(),
            LargeWKB => self.as_large_wkb().total_bounds(),
        }
    }
}

impl<G: NativeArray> TotalBounds for ChunkedGeometryArray<G> {
    fn total_bounds(&self) -> BoundingRect {
        let bounding_rects = self.map(|chunk| chunk.as_ref().total_bounds());
        bounding_rects
            .into_iter()
            .fold(BoundingRect::default(), |acc, x| acc + x)
    }
}

impl TotalBounds for &dyn ChunkedGeometryArrayTrait {
    fn total_bounds(&self) -> BoundingRect {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().total_bounds(),
            LineString(_, XY) => self.as_line_string::<2>().total_bounds(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().total_bounds(),
            Polygon(_, XY) => self.as_polygon::<2>().total_bounds(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().total_bounds(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().total_bounds(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().total_bounds(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().total_bounds(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().total_bounds(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().total_bounds(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().total_bounds(),
            Mixed(_, XY) => self.as_mixed::<2>().total_bounds(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().total_bounds(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().total_bounds(),
            LargeGeometryCollection(_, XY) => {
                self.as_large_geometry_collection::<2>().total_bounds()
            }
            Rect(XY) => self.as_rect::<2>().total_bounds(),
            Point(_, XYZ) => self.as_point::<3>().total_bounds(),
            LineString(_, XYZ) => self.as_line_string::<3>().total_bounds(),
            LargeLineString(_, XYZ) => self.as_large_line_string::<3>().total_bounds(),
            Polygon(_, XYZ) => self.as_polygon::<3>().total_bounds(),
            LargePolygon(_, XYZ) => self.as_large_polygon::<3>().total_bounds(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().total_bounds(),
            LargeMultiPoint(_, XYZ) => self.as_large_multi_point::<3>().total_bounds(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().total_bounds(),
            LargeMultiLineString(_, XYZ) => self.as_large_multi_line_string::<3>().total_bounds(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().total_bounds(),
            LargeMultiPolygon(_, XYZ) => self.as_large_multi_polygon::<3>().total_bounds(),
            Mixed(_, XYZ) => self.as_mixed::<3>().total_bounds(),
            LargeMixed(_, XYZ) => self.as_large_mixed::<3>().total_bounds(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().total_bounds(),
            LargeGeometryCollection(_, XYZ) => {
                self.as_large_geometry_collection::<3>().total_bounds()
            }
            Rect(XYZ) => self.as_rect::<3>().total_bounds(),
            WKB => self.as_wkb().total_bounds(),
            LargeWKB => self.as_large_wkb().total_bounds(),
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
    //     let dyn_arrs: Vec<Arc<dyn NativeArray>> =
    //         vec![Arc::new(polygon::p_array()), Arc::new(polygon::p_array())];
    //     let chunked_array: Arc<dyn ChunkedGeometryArrayTrait> =
    //         Arc::new(ChunkedGeometryArray::new(dyn_arrs));
    //     let total_bounds = chunked_array.as_ref().total_bounds();
    //     dbg!(total_bounds);
    // }
}
