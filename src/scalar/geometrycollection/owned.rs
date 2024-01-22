use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::MixedGeometryArray;
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::{Geometry, GeometryCollection};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Debug)]
pub struct OwnedGeometryCollection<O: OffsetSizeTrait> {
    array: MixedGeometryArray<O>,

    /// Offsets into the geometry array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedGeometryCollection<O> {
    pub fn new(
        array: MixedGeometryArray<O>,
        geom_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            array,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedGeometryCollection<O>> for GeometryCollection<'a, O> {
    fn from(value: &'a OwnedGeometryCollection<O>) -> Self {
        Self::new(&value.array, &value.geom_offsets, value.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedGeometryCollection<O>> for geo::GeometryCollection {
    fn from(value: &'a OwnedGeometryCollection<O>) -> Self {
        let geom = GeometryCollection::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<GeometryCollection<'a, O>> for OwnedGeometryCollection<O> {
    fn from(value: GeometryCollection<'a, O>) -> Self {
        let (array, geom_offsets, geom_index) = value.into_inner();
        Self::new(array.clone(), geom_offsets.clone(), geom_index)
    }
}

impl<O: OffsetSizeTrait> GeometryCollectionTrait for OwnedGeometryCollection<O> {
    type T = f64;
    type ItemType<'b> = Geometry<'b, O> where Self: 'b;

    fn num_geometries(&self) -> usize {
        GeometryCollection::from(self).num_geometries()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GeometryCollection::from(self).geometry_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> PartialEq<G>
    for OwnedGeometryCollection<O>
{
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}
