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

impl<T: CoordNum> MultiPolygonTrait for MultiPolygon<T> {
    type T = T;
    type ItemType<'a> = Polygon<Self::T> where Self: 'a;
    type Iter<'a> = Cloned<Iter<'a, Self::ItemType<'a>>> where T: 'a;

    fn polygons(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}

impl<'a, T: CoordNum> MultiPolygonTrait for &'a MultiPolygon<T> {
    type T = T;
    type ItemType<'b> = Polygon<Self::T> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn polygons(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}
