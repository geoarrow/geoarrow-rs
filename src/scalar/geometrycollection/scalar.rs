use crate::array::MixedGeometryArray;
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::geometrycollection::GeometryCollectionIterator;
use crate::scalar::Geometry;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a GeometryCollection
#[derive(Debug, Clone)]
pub struct GeometryCollection<'a, O: OffsetSizeTrait> {
    pub array: &'a MixedGeometryArray<O>,

    /// Offsets into the geometry array where each geometry starts
    pub geom_offsets: &'a OffsetBuffer<O>,

    pub geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait<'a> for GeometryCollection<'a, O> {
    type ScalarGeo = geo::GeometryCollection;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryCollectionTrait<'a> for GeometryCollection<'a, O> {
    type T = f64;
    type ItemType = Geometry<'a, O>;
    type Iter = GeometryCollectionIterator<'a, O>;

    fn geometries(&'a self) -> Self::Iter {
        GeometryCollectionIterator::new(self)
    }

    fn num_geometries(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn geometry(&self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(self.array.value(start + i))
    }
}

impl<O: OffsetSizeTrait> From<GeometryCollection<'_, O>> for geo::GeometryCollection {
    fn from(value: GeometryCollection<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&GeometryCollection<'_, O>> for geo::GeometryCollection {
    fn from(value: &GeometryCollection<'_, O>) -> Self {
        let num_geometries = value.num_geometries();
        let mut geoms: Vec<geo::Geometry> = Vec::with_capacity(num_geometries);
        for i in 0..value.num_geometries() {
            geoms.push(value.geometry(i).unwrap().into());
        }

        geo::GeometryCollection(geoms)
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
