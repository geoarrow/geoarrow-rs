use crate::algorithm::native::bounding_rect::{bounding_rect_geometry, bounding_rect_geometry_collection, bounding_rect_linestring, bounding_rect_multilinestring, bounding_rect_multipoint, bounding_rect_multipolygon, bounding_rect_polygon, bounding_rect_rect};
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PointTrait;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{OwnedRTree, RTreeBuilder};

pub trait RTree {
    type Output;

    fn create_rtree(&self) -> Self::Output {
        self.create_rtree_with_node_size(16)
    }

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output;
}

impl RTree for PointArray<2> {
    type Output = OwnedRTree<f64>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        assert_eq!(self.null_count(), 0);
        let mut builder = RTreeBuilder::new_with_node_size(self.len(), node_size);

        self.iter().flatten().for_each(|geom| {
            let (x, y) = geom.x_y();
            builder.add(x, y, x, y);
        });

        builder.finish::<HilbertSort>()
    }
}

impl RTree for RectArray<2> {
    type Output = OwnedRTree<f64>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        assert_eq!(self.null_count(), 0);
        let mut builder = RTreeBuilder::new_with_node_size(self.len(), node_size);

        self.iter().flatten().for_each(|geom| {
            let ([min_x, min_y], [max_x, max_y]) = bounding_rect_rect(&geom);
            builder.add(min_x, min_y, max_x, max_y);
        });

        builder.finish::<HilbertSort>()
    }
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

impl_rtree!(LineStringArray<2>, bounding_rect_linestring);
impl_rtree!(PolygonArray<2>, bounding_rect_polygon);
impl_rtree!(MultiPointArray<2>, bounding_rect_multipoint);
impl_rtree!(MultiLineStringArray<2>, bounding_rect_multilinestring);
impl_rtree!(MultiPolygonArray<2>, bounding_rect_multipolygon);
impl_rtree!(MixedGeometryArray<2>, bounding_rect_geometry);
impl_rtree!(GeometryCollectionArray<2>, bounding_rect_geometry_collection);

impl RTree for &dyn NativeArray {
    type Output = OwnedRTree<f64>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().create_rtree_with_node_size(node_size),
            LineString(_, XY) => self.as_line_string::<2>().create_rtree_with_node_size(node_size),
            Polygon(_, XY) => self.as_polygon::<2>().create_rtree_with_node_size(node_size),
            MultiPoint(_, XY) => self.as_multi_point::<2>().create_rtree_with_node_size(node_size),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().create_rtree_with_node_size(node_size),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().create_rtree_with_node_size(node_size),
            Mixed(_, XY) => self.as_mixed::<2>().create_rtree_with_node_size(node_size),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().create_rtree_with_node_size(node_size),
            Rect(XY) => self.as_rect::<2>().create_rtree_with_node_size(node_size),
            _ => todo!(),
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

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().create_rtree_with_node_size(node_size),
            LineString(_, XY) => self.as_line_string::<2>().create_rtree_with_node_size(node_size),
            Polygon(_, XY) => self.as_polygon::<2>().create_rtree_with_node_size(node_size),
            MultiPoint(_, XY) => self.as_multi_point::<2>().create_rtree_with_node_size(node_size),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().create_rtree_with_node_size(node_size),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().create_rtree_with_node_size(node_size),
            Mixed(_, XY) => self.as_mixed::<2>().create_rtree_with_node_size(node_size),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().create_rtree_with_node_size(node_size),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };

        Ok(result)
    }
}
