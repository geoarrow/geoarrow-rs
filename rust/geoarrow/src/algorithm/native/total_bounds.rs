use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::NativeType;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;

/// Computes the total bounds (extent) of the input.
pub trait TotalBounds {
    fn total_bounds(&self) -> BoundingRect;
}

impl TotalBounds for PointArray {
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
        impl TotalBounds for $type {
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

impl_array!(LineStringArray, add_line_string);
impl_array!(PolygonArray, add_polygon);
impl_array!(MultiPointArray, add_multi_point);
impl_array!(MultiLineStringArray, add_multi_line_string);
impl_array!(MultiPolygonArray, add_multi_polygon);
impl_array!(MixedGeometryArray, add_geometry);
impl_array!(GeometryCollectionArray, add_geometry_collection);

// impl<O: OffsetSizeTrait> TotalBounds for WKBArray<O> {
//     fn total_bounds(&self) -> BoundingRect {
//         let mut bounds = BoundingRect::new();
//         for geom in self.iter().flatten() {
//             bounds.add_geometry(&geom.to_wkb_object());
//         }
//         bounds
//     }
// }

impl TotalBounds for &dyn NativeArray {
    fn total_bounds(&self) -> BoundingRect {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().total_bounds(),
            LineString(_, _) => self.as_line_string().total_bounds(),
            Polygon(_, _) => self.as_polygon().total_bounds(),
            MultiPoint(_, _) => self.as_multi_point().total_bounds(),
            MultiLineString(_, _) => self.as_multi_line_string().total_bounds(),
            MultiPolygon(_, _) => self.as_multi_polygon().total_bounds(),
            Mixed(_, _) => self.as_mixed().total_bounds(),
            GeometryCollection(_, _) => self.as_geometry_collection().total_bounds(),
            Rect(_) => self.as_rect().total_bounds(),
            // WKB => self.as_wkb().total_bounds(),
            // LargeWKB => self.as_large_wkb().total_bounds(),
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

impl TotalBounds for &dyn ChunkedNativeArray {
    fn total_bounds(&self) -> BoundingRect {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().total_bounds(),
            LineString(_, _) => self.as_line_string().total_bounds(),
            Polygon(_, _) => self.as_polygon().total_bounds(),
            MultiPoint(_, _) => self.as_multi_point().total_bounds(),
            MultiLineString(_, _) => self.as_multi_line_string().total_bounds(),
            MultiPolygon(_, _) => self.as_multi_polygon().total_bounds(),
            Mixed(_, _) => self.as_mixed().total_bounds(),
            GeometryCollection(_, _) => self.as_geometry_collection().total_bounds(),
            Rect(_) => self.as_rect().total_bounds(),
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
        let chunked_array: Arc<dyn ChunkedNativeArray> = Arc::new(ChunkedGeometryArray::new(vec![
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
    //     let chunked_array: Arc<dyn ChunkedNativeArray> =
    //         Arc::new(ChunkedGeometryArray::new(dyn_arrs));
    //     let total_bounds = chunked_array.as_ref().total_bounds();
    //     dbg!(total_bounds);
    // }
}
