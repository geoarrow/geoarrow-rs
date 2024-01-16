use crate::algorithm::native::bounding_rect::{
    bounding_rect_geometry, bounding_rect_geometry_collection, bounding_rect_linestring,
    bounding_rect_multilinestring, bounding_rect_multipoint, bounding_rect_multipolygon,
    bounding_rect_polygon, bounding_rect_rect,
};
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::geo_traits::PointTrait;
use crate::GeometryArrayTrait;
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

impl RTree for PointArray {
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

impl RTree for RectArray {
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
        impl<O: OffsetSizeTrait> RTree for $struct_name {
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

impl_rtree!(LineStringArray<O>, bounding_rect_linestring);
impl_rtree!(PolygonArray<O>, bounding_rect_polygon);
impl_rtree!(MultiPointArray<O>, bounding_rect_multipoint);
impl_rtree!(MultiLineStringArray<O>, bounding_rect_multilinestring);
impl_rtree!(MultiPolygonArray<O>, bounding_rect_multipolygon);
impl_rtree!(MixedGeometryArray<O>, bounding_rect_geometry);
impl_rtree!(
    GeometryCollectionArray<O>,
    bounding_rect_geometry_collection
);

impl RTree for &dyn GeometryArrayTrait {
    type Output = OwnedRTree<f64>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        use GeoDataType::*;

        match self.data_type() {
            Point(_) => self.as_point().create_rtree_with_node_size(node_size),
            LineString(_) => self.as_line_string().create_rtree_with_node_size(node_size),
            LargeLineString(_) => self
                .as_large_line_string()
                .create_rtree_with_node_size(node_size),
            Polygon(_) => self.as_polygon().create_rtree_with_node_size(node_size),
            LargePolygon(_) => self
                .as_large_polygon()
                .create_rtree_with_node_size(node_size),
            MultiPoint(_) => self.as_multi_point().create_rtree_with_node_size(node_size),
            LargeMultiPoint(_) => self
                .as_large_multi_point()
                .create_rtree_with_node_size(node_size),
            MultiLineString(_) => self
                .as_multi_line_string()
                .create_rtree_with_node_size(node_size),
            LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .create_rtree_with_node_size(node_size),
            MultiPolygon(_) => self
                .as_multi_polygon()
                .create_rtree_with_node_size(node_size),
            LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .create_rtree_with_node_size(node_size),
            Mixed(_) => self.as_mixed().create_rtree_with_node_size(node_size),
            LargeMixed(_) => self.as_large_mixed().create_rtree_with_node_size(node_size),
            GeometryCollection(_) => self
                .as_geometry_collection()
                .create_rtree_with_node_size(node_size),
            LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .create_rtree_with_node_size(node_size),
            Rect => self.as_rect().create_rtree_with_node_size(node_size),
            _ => todo!(),
        }
    }
}
