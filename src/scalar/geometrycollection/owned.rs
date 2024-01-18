use crate::array::MixedGeometryArray;
use crate::scalar::GeometryCollection;
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
