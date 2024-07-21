use super::iterator::MultiPolygonIterator;
use super::polygon::PolygonTrait;
use geo::{CoordNum, MultiPolygon, Polygon};

/// A trait for accessing data from a generic MultiPolygon.
pub trait MultiPolygonTrait<const DIM: usize>: Sized {
    type T: CoordNum;
    type ItemType<'a>: 'a + PolygonTrait<DIM, T = Self::T>
    where
        Self: 'a;

    /// Native dimension of the coordinate tuple
    fn dim(&self) -> usize {
        DIM
    }

    /// An iterator over the Polygons in this MultiPolygon
    fn polygons(&self) -> MultiPolygonIterator<'_, Self::T, DIM, Self::ItemType<'_>, Self> {
        MultiPolygonIterator::new(self, 0, self.num_polygons())
    }

    /// The number of polygons in this MultiPolygon
    fn num_polygons(&self) -> usize;

    /// Access to a specified polygon in this MultiPolygon
    /// Will return None if the provided index is out of bounds
    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i >= self.num_polygons() {
            None
        } else {
            unsafe { Some(self.polygon_unchecked(i)) }
        }
    }

    /// Access to a specified polygon in this MultiPolygon
    ///
    /// # Safety
    ///
    /// Accessing an index out of bounds is UB.
    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_>;
}

impl<T: CoordNum> MultiPolygonTrait<2> for MultiPolygon<T> {
    type T = T;
    type ItemType<'a> = &'a Polygon<Self::T> where Self: 'a;

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.0.get_unchecked(i)
    }
}

impl<'a, T: CoordNum> MultiPolygonTrait<2> for &'a MultiPolygon<T> {
    type T = T;
    type ItemType<'b> = &'a Polygon<Self::T> where Self: 'b;

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.0.get_unchecked(i)
    }
}
