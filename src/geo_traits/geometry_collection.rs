use super::GeometryTrait;
use geo::{CoordNum, Geometry, GeometryCollection};
use std::iter::Cloned;
use std::slice::Iter;

pub trait GeometryCollectionTrait {
    type T: CoordNum;
    type ItemType<'a>: 'a + GeometryTrait<T = Self::T>;
    type Iter<'a>: ExactSizeIterator<Item = Self::ItemType<'a>>;

    /// An iterator over the geometries in this GeometryCollection
    fn geometries(&self) -> Self::Iter<'_>;

    /// The number of geometries in this GeometryCollection
    fn num_geometries(&self) -> usize;

    /// Access to a specified geometry in this GeometryCollection
    /// Will return None if the provided index is out of bounds
    fn geometry(&self, i: usize) -> Option<Self::ItemType<'_>>;
}

impl<'a, T: CoordNum + 'a> GeometryCollectionTrait for GeometryCollection<T> {
    type T = T;
    type ItemType = Geometry<Self::T>;
    type Iter = Cloned<Iter<'a, Self::ItemType<'a>>>;

    fn geometries(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_geometries(&self) -> usize {
        self.0.len()
    }

    fn geometry(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}

impl<'a, T: CoordNum + 'a> GeometryCollectionTrait for &GeometryCollection<T> {
    type T = T;
    type ItemType = Geometry<Self::T>;
    type Iter = Cloned<Iter<'a, Self::ItemType<'a>>>;

    fn geometries(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_geometries(&self) -> usize {
        self.0.len()
    }

    fn geometry(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}
