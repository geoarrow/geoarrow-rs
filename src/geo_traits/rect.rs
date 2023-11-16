use geo::{Coord, CoordNum, Rect};

use crate::geo_traits::CoordTrait;

pub trait RectTrait {
    type T: CoordNum;
    type ItemType<'a>: 'a + CoordTrait<T = Self::T>;

    fn lower(&self) -> Self::ItemType<'_>;

    fn upper(&self) -> Self::ItemType<'_>;
}

impl<'a, T: CoordNum + 'a> RectTrait for Rect<T> {
    type T = T;
    type ItemType = Coord<T>;

    fn lower(&self) -> Self::ItemType<'_> {
        self.min()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        self.max()
    }
}

impl<'a, T: CoordNum + 'a> RectTrait for &Rect<T> {
    type T = T;
    type ItemType = Coord<T>;

    fn lower(&self) -> Self::ItemType<'_> {
        self.min()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        self.max()
    }
}
