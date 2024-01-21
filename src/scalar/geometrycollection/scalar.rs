use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::MixedGeometryArray;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::geo::geometry_collection_to_geo;
use crate::scalar::Geometry;
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::GeometryScalarTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a GeometryCollection
#[derive(Debug, Clone)]
pub struct GeometryCollection<'a, O: OffsetSizeTrait> {
    pub(crate) array: &'a MixedGeometryArray<O>,

    /// Offsets into the geometry array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<O>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, O: OffsetSizeTrait> GeometryCollection<'a, O> {
    pub fn new(
        array: &'a MixedGeometryArray<O>,
        geom_offsets: &'a OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            array,
            geom_offsets,
            geom_index,
            start_offset,
        }
    }

    pub fn into_inner(&self) -> (&MixedGeometryArray<O>, &OffsetBuffer<O>, usize) {
        (self.array, self.geom_offsets, self.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for GeometryCollection<'a, O> {
    type ScalarGeo = geo::GeometryCollection;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryCollectionTrait for GeometryCollection<'a, O> {
    type T = f64;
    type ItemType<'b> = Geometry<'a, O> where Self: 'b;

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.array.value(self.start_offset + i)
    }
}

impl<'a, O: OffsetSizeTrait> GeometryCollectionTrait for &'a GeometryCollection<'a, O> {
    type T = f64;
    type ItemType<'b> = Geometry<'a, O> where Self: 'b;

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.array.value(self.start_offset + i)
    }
}

// impl<O: OffsetSizeTrait> From<GeometryCollection<'_, O>> for geo::GeometryCollection {
//     fn from(value: GeometryCollection<'_, O>) -> Self {
//         (&value).into()
//     }
// }

impl<O: OffsetSizeTrait> From<&GeometryCollection<'_, O>> for geo::GeometryCollection {
    fn from(value: &GeometryCollection<'_, O>) -> Self {
        geometry_collection_to_geo(value)
    }
}

impl<O: OffsetSizeTrait> From<GeometryCollection<'_, O>> for geo::Geometry {
    fn from(value: GeometryCollection<'_, O>) -> Self {
        geo::Geometry::GeometryCollection(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for GeometryCollection<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        todo!()
        // let (lower, upper) = bounding_rect_multilinestring(self);
        // AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> PartialEq<G>
    for GeometryCollection<'_, O>
{
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}
