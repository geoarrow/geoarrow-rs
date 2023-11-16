use super::polygon::PolygonTrait;
use geo::{CoordNum, MultiPolygon, Polygon};
use std::iter::Cloned;
use std::slice::Iter;

pub trait MultiPolygonTrait {
    type T: CoordNum;
    type ItemType<'a>: 'a + PolygonTrait<T = Self::T>
    where
        Self: 'a;
    type Iter<'a>: ExactSizeIterator<Item = Self::ItemType<'a>>
    where
        Self: 'a;

    /// An iterator over the Polygons in this MultiPolygon
    fn polygons(&self) -> Self::Iter<'_>;

    /// The number of polygons in this MultiPolygon
    fn num_polygons(&self) -> usize;

    /// Access to a specified polygon in this MultiPolygon
    /// Will return None if the provided index is out of bounds
    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>>;
}

impl<'a, T: CoordNum + 'a> MultiPolygonTrait for MultiPolygon<T> {
    type T = T;
    type ItemType = Polygon<Self::T>;
    type Iter = Cloned<Iter<'a, Self::ItemType<'a>>>;

    fn polygons(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'a>> {
        self.0.get(i).cloned()
    }
}

impl<'a, T: CoordNum + 'a> MultiPolygonTrait for &MultiPolygon<T> {
    type T = T;
    type ItemType = Polygon<Self::T>;
    type Iter = Cloned<Iter<'a, Self::ItemType<'a>>>;

    fn polygons(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'a>> {
        self.0.get(i).cloned()
    }
}
