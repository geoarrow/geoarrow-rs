use crate::array::*;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use flatbush::flatbush::sort::Sort;
use flatbush::flatbush::HilbertSort;
use flatbush::{FlatbushBuilder, OwnedFlatbush};
use rstar::RTreeObject;

pub trait FlatbushRTree {
    type Output;
    type SortMethod: Sort;

    fn flatbush(&self) -> Self::Output {
        self.flatbush_with_node_size(16)
    }

    fn flatbush_with_node_size(&self, node_size: usize) -> Self::Output;
}

impl FlatbushRTree for PointArray {
    type Output = OwnedFlatbush;
    type SortMethod = HilbertSort;

    fn flatbush_with_node_size(&self, node_size: usize) -> Self::Output {
        if self.null_count() > 0 {
            panic!("null count>0 not supported");
        }

        let mut builder = FlatbushBuilder::new_with_node_size(self.len(), node_size);

        for geom in self.iter().flatten() {
            let envelope = geom.envelope();
            let [min_x, min_y] = envelope.lower();
            let [max_x, max_y] = envelope.upper();
            builder.add(min_x, min_y, max_x, max_y);
        }

        builder.finish::<Self::SortMethod>()
    }
}

macro_rules! impl_array {
    ($array_name:ty) => {
        impl<O: OffsetSizeTrait> FlatbushRTree for $array_name {
            type Output = OwnedFlatbush;
            type SortMethod = HilbertSort;

            fn flatbush_with_node_size(&self, node_size: usize) -> Self::Output {
                if self.null_count() > 0 {
                    panic!("null count>0 not supported");
                }

                let mut builder = FlatbushBuilder::new_with_node_size(self.len(), node_size);

                for geom in self.iter().flatten() {
                    let envelope = geom.envelope();
                    let [min_x, min_y] = envelope.lower();
                    let [max_x, max_y] = envelope.upper();
                    builder.add(min_x, min_y, max_x, max_y);
                }

                builder.finish::<Self::SortMethod>()
            }
        }
    };
}

impl_array!(LineStringArray<O>);
impl_array!(PolygonArray<O>);
impl_array!(MultiPointArray<O>);
impl_array!(MultiLineStringArray<O>);
impl_array!(MultiPolygonArray<O>);
impl_array!(MixedGeometryArray<O>);
impl_array!(GeometryCollectionArray<O>);
