use super::iterator::GeometryCollectionIterator;
use super::GeometryTrait;
use geo::{CoordNum, Geometry, GeometryCollection};

/// A trait for accessing data from a generic GeometryCollection.
pub trait GeometryCollectionTrait<const DIM: usize>: Sized {
    type T: CoordNum;
    type ItemType<'a>: 'a + GeometryTrait<DIM, T = Self::T>
    where
        Self: 'a;

    /// Native dimension of the coordinate tuple
    fn dim(&self) -> usize {
        DIM
    }

    /// An iterator over the geometries in this GeometryCollection
    fn geometries(&self) -> GeometryCollectionIterator<'_, Self::T, DIM, Self::ItemType<'_>, Self> {
        GeometryCollectionIterator::new(self, 0, self.num_geometries())
    }

    /// The number of geometries in this GeometryCollection
    fn num_geometries(&self) -> usize;

    /// Access to a specified geometry in this GeometryCollection
    /// Will return None if the provided index is out of bounds
    fn geometry(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i >= self.num_geometries() {
            None
        } else {
            unsafe { Some(self.geometry_unchecked(i)) }
        }
    }

    /// Access to a specified geometry in this GeometryCollection
    ///
    /// # Safety
    ///
    /// Accessing an index out of bounds is UB.
    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_>;
}

impl<T: CoordNum> GeometryCollectionTrait<2> for GeometryCollection<T> {
    type T = T;
    type ItemType<'a> = &'a Geometry<Self::T>
    where
        Self: 'a;

    fn num_geometries(&self) -> usize {
        self.0.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.0.get_unchecked(i)
    }
}

impl<'a, T: CoordNum> GeometryCollectionTrait<2> for &'a GeometryCollection<T> {
    type T = T;
    type ItemType<'b> = &'a Geometry<Self::T> where
        Self: 'b;

    fn num_geometries(&self) -> usize {
        self.0.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.0.get_unchecked(i)
    }
}
