use crate::algorithm::native::eq::geometry_collection_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::MixedGeometryArray;
use crate::geo_traits::GeometryCollectionTrait;
use crate::io::geo::geometry_collection_to_geo;
use crate::scalar::Geometry;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a GeometryCollection
#[derive(Debug, Clone)]
pub struct GeometryCollection<'a, const D: usize> {
    pub(crate) array: &'a MixedGeometryArray<D>,

    /// Offsets into the geometry array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, const D: usize> GeometryCollection<'a, D> {
    pub fn new(array: &'a MixedGeometryArray<D>, geom_offsets: &'a OffsetBuffer<i32>, geom_index: usize) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self { array, geom_offsets, geom_index, start_offset }
    }

    pub fn into_inner(&self) -> (&MixedGeometryArray<D>, &OffsetBuffer<i32>, usize) {
        (self.array, self.geom_offsets, self.geom_index)
    }
}

impl<'a, const D: usize> NativeScalar for GeometryCollection<'a, D> {
    type ScalarGeo = geo::GeometryCollection;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::GeometryCollection(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, const D: usize> GeometryCollectionTrait for GeometryCollection<'a, D> {
    type T = f64;
    type ItemType<'b> = Geometry<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.array.value(self.start_offset + i)
    }
}

impl<'a, const D: usize> GeometryCollectionTrait for &'a GeometryCollection<'a, D> {
    type T = f64;
    type ItemType<'b> = Geometry<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.array.value(self.start_offset + i)
    }
}

// impl<O: OffsetSizeTrait> From<GeometryCollection<'_, 2>> for geo::GeometryCollection {
//     fn from(value: GeometryCollection<'_, 2>) -> Self {
//         (&value).into()
//     }
// }

impl<const D: usize> From<&GeometryCollection<'_, D>> for geo::GeometryCollection {
    fn from(value: &GeometryCollection<'_, D>) -> Self {
        geometry_collection_to_geo(value)
    }
}

impl<const D: usize> From<GeometryCollection<'_, D>> for geo::Geometry {
    fn from(value: GeometryCollection<'_, D>) -> Self {
        geo::Geometry::GeometryCollection(value.into())
    }
}

impl RTreeObject for GeometryCollection<'_, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        todo!()
        // let (lower, upper) = bounding_rect_multilinestring(self);
        // AABB::from_corners(lower, upper)
    }
}

impl<const D: usize, G: GeometryCollectionTrait<T = f64>> PartialEq<G> for GeometryCollection<'_, D> {
    fn eq(&self, other: &G) -> bool {
        geometry_collection_eq(self, other)
    }
}
