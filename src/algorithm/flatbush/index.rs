use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use flatbush::flatbush::sort::Sort;
use flatbush::flatbush::HilbertSort;
use flatbush::{FlatbushBuilder, OwnedFlatbush};
use rstar::RTreeObject;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexedGeometryArray<G: GeometryArrayTrait> {
    index: OwnedFlatbush,
    array: G,
}

impl<G: GeometryArrayTrait> IndexedGeometryArray<G> {
    pub fn from_array(array: G) -> Result<Self> {
        let index = array.as_ref().flatbush()?;
        Ok(Self { index, array })
    }

    pub fn from_array_with_node_size(array: G, node_size: usize) -> Result<Self> {
        let index = array.as_ref().flatbush_with_node_size(node_size)?;
        Ok(Self { index, array })
    }
}

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

impl FlatbushRTree for &dyn GeometryArrayTrait {
    type Output = Result<OwnedFlatbush>;
    type SortMethod = HilbertSort;

    fn flatbush_with_node_size(&self, node_size: usize) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().flatbush_with_node_size(node_size),
            GeoDataType::LineString(_) => self.as_line_string().flatbush_with_node_size(node_size),
            GeoDataType::LargeLineString(_) => self
                .as_large_line_string()
                .flatbush_with_node_size(node_size),
            GeoDataType::Polygon(_) => self.as_polygon().flatbush_with_node_size(node_size),
            GeoDataType::LargePolygon(_) => {
                self.as_large_polygon().flatbush_with_node_size(node_size)
            }
            GeoDataType::MultiPoint(_) => self.as_multi_point().flatbush_with_node_size(node_size),
            GeoDataType::LargeMultiPoint(_) => self
                .as_large_multi_point()
                .flatbush_with_node_size(node_size),
            GeoDataType::MultiLineString(_) => self
                .as_multi_line_string()
                .flatbush_with_node_size(node_size),
            GeoDataType::LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .flatbush_with_node_size(node_size),
            GeoDataType::MultiPolygon(_) => {
                self.as_multi_polygon().flatbush_with_node_size(node_size)
            }
            GeoDataType::LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .flatbush_with_node_size(node_size),
            GeoDataType::Mixed(_) => self.as_mixed().flatbush_with_node_size(node_size),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().flatbush_with_node_size(node_size),
            GeoDataType::GeometryCollection(_) => self
                .as_geometry_collection()
                .flatbush_with_node_size(node_size),
            GeoDataType::LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .flatbush_with_node_size(node_size),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
