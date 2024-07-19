use crate::algorithm::native::bounding_rect::{
    bounding_rect_geometry, bounding_rect_geometry_collection, bounding_rect_linestring,
    bounding_rect_multilinestring, bounding_rect_multipoint, bounding_rect_multipolygon,
    bounding_rect_polygon, bounding_rect_rect,
};
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PointTrait;
use crate::trait_::GeometryArrayAccessor;
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

impl_rtree!(LineStringArray<O, 2>, bounding_rect_linestring);
impl_rtree!(PolygonArray<O, 2>, bounding_rect_polygon);
impl_rtree!(MultiPointArray<O, 2>, bounding_rect_multipoint);
impl_rtree!(MultiLineStringArray<O, 2>, bounding_rect_multilinestring);
impl_rtree!(MultiPolygonArray<O, 2>, bounding_rect_multipolygon);
impl_rtree!(MixedGeometryArray<O, 2>, bounding_rect_geometry);
impl_rtree!(
    GeometryCollectionArray<O, 2>,
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

impl<G: GeometryArrayTrait> RTree for ChunkedGeometryArray<G> {
    type Output = Vec<OwnedRTree<f64>>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        self.map(|chunk| chunk.as_ref().create_rtree_with_node_size(node_size))
    }
}

impl RTree for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Vec<OwnedRTree<f64>>>;

    fn create_rtree_with_node_size(&self, node_size: usize) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().create_rtree_with_node_size(node_size),
            GeoDataType::LineString(_) => {
                self.as_line_string().create_rtree_with_node_size(node_size)
            }
            GeoDataType::LargeLineString(_) => self
                .as_large_line_string()
                .create_rtree_with_node_size(node_size),
            GeoDataType::Polygon(_) => self.as_polygon().create_rtree_with_node_size(node_size),
            GeoDataType::LargePolygon(_) => self
                .as_large_polygon()
                .create_rtree_with_node_size(node_size),
            GeoDataType::MultiPoint(_) => {
                self.as_multi_point().create_rtree_with_node_size(node_size)
            }
            GeoDataType::LargeMultiPoint(_) => self
                .as_large_multi_point()
                .create_rtree_with_node_size(node_size),
            GeoDataType::MultiLineString(_) => self
                .as_multi_line_string()
                .create_rtree_with_node_size(node_size),
            GeoDataType::LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .create_rtree_with_node_size(node_size),
            GeoDataType::MultiPolygon(_) => self
                .as_multi_polygon()
                .create_rtree_with_node_size(node_size),
            GeoDataType::LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .create_rtree_with_node_size(node_size),
            GeoDataType::Mixed(_) => self.as_mixed().create_rtree_with_node_size(node_size),
            GeoDataType::LargeMixed(_) => {
                self.as_large_mixed().create_rtree_with_node_size(node_size)
            }
            GeoDataType::GeometryCollection(_) => self
                .as_geometry_collection()
                .create_rtree_with_node_size(node_size),
            GeoDataType::LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .create_rtree_with_node_size(node_size),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };

        Ok(result)
    }
}
