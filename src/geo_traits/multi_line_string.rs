use super::line_string::LineStringTrait;
use geo::{CoordNum, LineString, MultiLineString};
use std::iter::Cloned;
use std::slice::Iter;

pub trait MultiLineStringTrait {
    type T: CoordNum;
    type ItemType<'a>: 'a + LineStringTrait<T = Self::T>
    where
        Self: 'a;
    type Iter<'a>: ExactSizeIterator<Item = Self::ItemType<'a>>
    where
        Self: 'a;

    /// An iterator over the LineStrings in this MultiLineString
    fn lines(&self) -> Self::Iter<'_>;

    /// The number of lines in this MultiLineString
    fn num_lines(&self) -> usize;

    /// Access to a specified line in this MultiLineString
    /// Will return None if the provided index is out of bounds
    fn line(&self, i: usize) -> Option<Self::ItemType<'_>>;
}

impl<'a, T: CoordNum + 'a> MultiLineStringTrait for MultiLineString<T> {
    type T = T;
    type ItemType = LineString<Self::T>;
    type Iter = Cloned<Iter<'a, Self::ItemType<'a>>>;

    fn lines(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_lines(&self) -> usize {
        self.0.len()
    }

    fn line(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}

impl<'a, T: CoordNum + 'a> MultiLineStringTrait for &MultiLineString<T> {
    type T = T;
    type ItemType = LineString<Self::T>;
    type Iter = Cloned<Iter<'a, Self::ItemType<'a>>>;

    fn lines(&self) -> Self::Iter<'_> {
        self.0.iter().cloned()
    }

    fn num_lines(&self) -> usize {
        self.0.len()
    }

    fn line(&self, i: usize) -> Option<Self::ItemType<'_>> {
        self.0.get(i).cloned()
    }
}
