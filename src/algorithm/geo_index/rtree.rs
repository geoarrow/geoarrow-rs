use crate::algorithm::native::bounding_rect::{
    bounding_rect_geometry, bounding_rect_geometry_collection, bounding_rect_linestring,
    bounding_rect_multilinestring, bounding_rect_multipoint, bounding_rect_multipolygon,
    bounding_rect_point, bounding_rect_polygon, bounding_rect_rect,
};
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
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
        impl<const D: usize> RTree for $struct_name {
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

impl_rtree!(PointArray<D>, bounding_rect_point);
impl_rtree!(LineStringArray<D>, bounding_rect_linestring);
impl_rtree!(PolygonArray<D>, bounding_rect_polygon);
impl_rtree!(MultiPointArray<D>, bounding_rect_multipoint);
impl_rtree!(MultiLineStringArray<D>, bounding_rect_multilinestring);
impl_rtree!(MultiPolygonArray<D>, bounding_rect_multipolygon);
impl_rtree!(MixedGeometryArray<D>, bounding_rect_geometry);
impl_rtree!(
    GeometryCollectionArray<D>,
    bounding_rect_geometry_collection
);
impl_rtree!(RectArray<D>, bounding_rect_rect);

impl RTree for &dyn NativeArray {
    type Output = OwnedRTree<f64>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        macro_rules! impl_method {
            ($method:ident, $dim:expr) => {
                self.$method::<$dim>()
                    .create_rtree_with_node_size(node_size)
            };
        }

        match self.data_type() {
            Point(_, XY) => impl_method!(as_point, 2),
            LineString(_, XY) => impl_method!(as_line_string, 2),
            Polygon(_, XY) => impl_method!(as_polygon, 2),
            MultiPoint(_, XY) => impl_method!(as_multi_point, 2),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string, 2),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon, 2),
            Mixed(_, XY) => impl_method!(as_mixed, 2),
            GeometryCollection(_, XY) => impl_method!(as_geometry_collection, 2),
            Rect(XY) => impl_method!(as_rect, 2),
            Point(_, XYZ) => impl_method!(as_point, 3),
            LineString(_, XYZ) => impl_method!(as_line_string, 3),
            Polygon(_, XYZ) => impl_method!(as_polygon, 3),
            MultiPoint(_, XYZ) => impl_method!(as_multi_point, 3),
            MultiLineString(_, XYZ) => impl_method!(as_multi_line_string, 3),
            MultiPolygon(_, XYZ) => impl_method!(as_multi_polygon, 3),
            Mixed(_, XYZ) => impl_method!(as_mixed, 3),
            GeometryCollection(_, XYZ) => impl_method!(as_geometry_collection, 3),
            Rect(XYZ) => impl_method!(as_rect, 3),
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
        use Dimension::*;
        use NativeType::*;

        macro_rules! impl_method {
            ($method:ident, $dim:expr) => {
                self.$method::<$dim>()
                    .create_rtree_with_node_size(node_size)
            };
        }

        let result = match self.data_type() {
            Point(_, XY) => impl_method!(as_point, 2),
            LineString(_, XY) => impl_method!(as_line_string, 2),
            Polygon(_, XY) => impl_method!(as_polygon, 2),
            MultiPoint(_, XY) => impl_method!(as_multi_point, 2),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string, 2),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon, 2),
            Mixed(_, XY) => impl_method!(as_mixed, 2),
            GeometryCollection(_, XY) => impl_method!(as_geometry_collection, 2),
            Rect(XY) => impl_method!(as_rect, 2),
            Point(_, XYZ) => impl_method!(as_point, 3),
            LineString(_, XYZ) => impl_method!(as_line_string, 3),
            Polygon(_, XYZ) => impl_method!(as_polygon, 3),
            MultiPoint(_, XYZ) => impl_method!(as_multi_point, 3),
            MultiLineString(_, XYZ) => impl_method!(as_multi_line_string, 3),
            MultiPolygon(_, XYZ) => impl_method!(as_multi_polygon, 3),
            Mixed(_, XYZ) => impl_method!(as_mixed, 3),
            GeometryCollection(_, XYZ) => impl_method!(as_geometry_collection, 3),
            Rect(XYZ) => impl_method!(as_rect, 3),
        };
        Ok(result)
    }
}
