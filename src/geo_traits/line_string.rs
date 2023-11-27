use super::CoordTrait;
use geo::{Coord, CoordNum, LineString};
use std::iter::Cloned;
use std::slice::Iter;

pub trait LineStringTrait {
    type T: CoordNum;
    type ItemType<'a>: 'a + CoordTrait<T = Self::T>
    where
        Self: 'a;
    type Iter<'a>: ExactSizeIterator<Item = Self::ItemType<'a>>
    where
        Self: 'a;

    /// An iterator over the coords in this LineString
    fn coords(&self) -> Self::Iter<'_>;

    /// The number of coords in this LineString
    fn num_coords(&self) -> usize;

    /// Access to a specified point in this LineString
    /// Will return None if the provided index is out of bounds
    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>>;
}

pub struct LineStringIterator<'a, T: CoordNum> {
    iter: std::slice::Iter<'a, Coord<T>>,
}

impl<'a, T: CoordNum> Iterator for LineStringIterator<'a, T> {
    type Item = &'a Coord<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, T: CoordNum> ExactSizeIterator for LineStringIterator<'a, T> {
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<T: CoordNum> LineStringTrait for LineString<T> {
    type T = T;
    type ItemType<'a> = &'a Coord<Self::T> where Self: 'a;
    type Iter<'a> = LineStringIterator<'a, T> where T: 'a;

    fn coords(&self) -> Self::Iter<'_> {
        LineStringIterator {
            iter: self.0.iter(),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i)
    }
}

impl<'a, T: CoordNum> LineStringTrait for &'a LineString<T> {
    type T = T;
    type ItemType<'b> = Coord<Self::T> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn coords(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}
