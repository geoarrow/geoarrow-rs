use geo::{Coord, CoordNum, Rect};

use crate::geo_traits::CoordTrait;

/// A trait for accessing data from a generic Rect.
pub trait RectTrait<const DIM: usize> {
    type T: CoordNum;
    type ItemType<'a>: 'a + CoordTrait<DIM, T = Self::T>
    where
        Self: 'a;

    /// Native dimension of the coordinate tuple
    fn dim(&self) -> usize {
        DIM
    }

    fn lower(&self) -> Self::ItemType<'_>;

    fn upper(&self) -> Self::ItemType<'_>;
}

impl<'a, T: CoordNum + 'a> RectTrait<2> for Rect<T> {
    type T = T;
    type ItemType<'b> = Coord<T> where Self: 'b;

    fn lower(&self) -> Self::ItemType<'_> {
        self.min()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        self.max()
    }
}

impl<'a, T: CoordNum + 'a> RectTrait<2> for &'a Rect<T> {
    type T = T;
    type ItemType<'b> = Coord<T> where Self: 'b;

    fn lower(&self) -> Self::ItemType<'_> {
        self.min()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        self.max()
    }
}
