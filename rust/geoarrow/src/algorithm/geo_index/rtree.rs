use crate::algorithm::native::bounding_rect::{
    bounding_rect_geometry, bounding_rect_geometry_collection, bounding_rect_linestring,
    bounding_rect_multilinestring, bounding_rect_multipoint, bounding_rect_multipolygon,
    bounding_rect_point, bounding_rect_polygon, bounding_rect_rect,
};
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{OwnedRTree, RTreeBuilder};

pub trait RTree {
    type Output;

    fn create_rtree(&self) -> Self::Output {
        self.create_rtree_with_node_size(16)
    }

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output;
}

macro_rules! impl_rtree {
    ($struct_name:ty, $bounding_rect_fn:ident) => {
        impl RTree for $struct_name {
            type Output = OwnedRTree<f64>;

            fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
                assert_eq!(self.null_count(), 0);
                let mut builder = RTreeBuilder::new_with_node_size(self.len(), node_size);

                self.iter().flatten().for_each(|geom| {
                    let ([min_x, min_y], [max_x, max_y]) = $bounding_rect_fn(&geom);
                    builder.add(min_x, min_y, max_x, max_y);
                });

                builder.finish::<HilbertSort>()
            }
        }
    };
}

impl_rtree!(PointArray, bounding_rect_point);
impl_rtree!(LineStringArray, bounding_rect_linestring);
impl_rtree!(PolygonArray, bounding_rect_polygon);
impl_rtree!(MultiPointArray, bounding_rect_multipoint);
impl_rtree!(MultiLineStringArray, bounding_rect_multilinestring);
impl_rtree!(MultiPolygonArray, bounding_rect_multipolygon);
impl_rtree!(MixedGeometryArray, bounding_rect_geometry);
impl_rtree!(GeometryCollectionArray, bounding_rect_geometry_collection);
impl_rtree!(RectArray, bounding_rect_rect);
impl_rtree!(GeometryArray, bounding_rect_geometry);

impl RTree for &dyn NativeArray {
    type Output = OwnedRTree<f64>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        use NativeType::*;

        macro_rules! impl_method {
            ($method:ident) => {
                self.$method().create_rtree_with_node_size(node_size)
            };
        }

        match self.data_type() {
            Point(_, _) => impl_method!(as_point),
            LineString(_, _) => impl_method!(as_line_string),
            Polygon(_, _) => impl_method!(as_polygon),
            MultiPoint(_, _) => impl_method!(as_multi_point),
            MultiLineString(_, _) => impl_method!(as_multi_line_string),
            MultiPolygon(_, _) => impl_method!(as_multi_polygon),
            GeometryCollection(_, _) => impl_method!(as_geometry_collection),
            Rect(_) => impl_method!(as_rect),
            Geometry(_) => impl_method!(as_geometry),
        }
    }
}

impl<G: NativeArray> RTree for ChunkedGeometryArray<G> {
    type Output = Vec<OwnedRTree<f64>>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        self.map(|chunk| chunk.as_ref().create_rtree_with_node_size(node_size))
    }
}

impl RTree for &dyn ChunkedNativeArray {
    type Output = Result<Vec<OwnedRTree<f64>>>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        use NativeType::*;

        macro_rules! impl_method {
            ($method:ident) => {
                self.$method().create_rtree_with_node_size(node_size)
            };
        }

        let result = match self.data_type() {
            Point(_, _) => impl_method!(as_point),
            LineString(_, _) => impl_method!(as_line_string),
            Polygon(_, _) => impl_method!(as_polygon),
            MultiPoint(_, _) => impl_method!(as_multi_point),
            MultiLineString(_, _) => impl_method!(as_multi_line_string),
            MultiPolygon(_, _) => impl_method!(as_multi_polygon),
            GeometryCollection(_, _) => impl_method!(as_geometry_collection),
            Rect(_) => impl_method!(as_rect),
            Geometry(_) => todo!("Chunked unknown array"), // impl_method!(as_unknown),
        };
        Ok(result)
    }
}
