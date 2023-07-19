use geo::{Coord, CoordNum, Rect};

use crate::geo_traits::CoordTrait;

pub trait RectTrait<'a> {
    type T: CoordNum;
    type ItemType: 'a + CoordTrait<T = Self::T>;

    fn lower(&self) -> Self::ItemType;

    fn upper(&self) -> Self::ItemType;
}

impl<'a, T: CoordNum + 'a> RectTrait<'a> for Rect<T> {
    type T = T;
    type ItemType = Coord<T>;

    fn lower(&self) -> Self::ItemType {
        self.min()
    }

    fn upper(&self) -> Self::ItemType {
        self.max()
    }
}

impl<'a, T: CoordNum + 'a> RectTrait<'a> for &Rect<T> {
    type T = T;
    type ItemType = Coord<T>;

    fn lower(&self) -> Self::ItemType {
        self.min()
    }

    fn upper(&self) -> Self::ItemType {
        self.max()
    }
}
