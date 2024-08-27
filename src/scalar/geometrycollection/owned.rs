use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::{GeometryCollectionArray, MixedGeometryArray};
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::{Geometry, GeometryCollection};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedGeometryCollection<O: OffsetSizeTrait, const D: usize> {
    array: MixedGeometryArray<O, D>,

    /// Offsets into the geometry array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait, const D: usize> OwnedGeometryCollection<O, D> {
    pub fn new(
        array: MixedGeometryArray<O, D>,
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

impl<'a, O: OffsetSizeTrait, const D: usize> From<&'a OwnedGeometryCollection<O, D>>
    for GeometryCollection<'a, O, D>
{
    fn from(value: &'a OwnedGeometryCollection<O, D>) -> Self {
        Self::new(&value.array, &value.geom_offsets, value.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedGeometryCollection<O, 2>> for geo::GeometryCollection {
    fn from(value: &'a OwnedGeometryCollection<O, 2>) -> Self {
        let geom = GeometryCollection::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<GeometryCollection<'a, O, D>>
    for OwnedGeometryCollection<O, D>
{
    fn from(value: GeometryCollection<'a, O, D>) -> Self {
        let (array, geom_offsets, geom_index) = value.into_inner();
        Self::new(array.clone(), geom_offsets.clone(), geom_index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<OwnedGeometryCollection<O, D>>
    for GeometryCollectionArray<O, D>
{
    fn from(value: OwnedGeometryCollection<O, D>) -> Self {
        Self::new(value.array, value.geom_offsets, None, Default::default())
    }
}

impl<O: OffsetSizeTrait, const D: usize> GeometryCollectionTrait for OwnedGeometryCollection<O, D> {
    type T = f64;
    type ItemType<'b> = Geometry<'b, O, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_geometries(&self) -> usize {
        GeometryCollection::from(self).num_geometries()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GeometryCollection::from(self).geometry_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> PartialEq<G>
    for OwnedGeometryCollection<O, 2>
{
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}
