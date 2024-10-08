use super::iterator::MultiLineStringIterator;
use super::line_string::LineStringTrait;
use geo::{CoordNum, LineString, MultiLineString};

/// A trait for accessing data from a generic MultiLineString.
pub trait MultiLineStringTrait: Sized {
    /// The coordinate type of this geometry
    type T: CoordNum;

    /// The type of each underlying LineString, which implements [LineStringTrait]
    type ItemType<'a>: 'a + LineStringTrait<T = Self::T>
    where
        Self: 'a;

    /// The number of dimensions in this geometry
    fn dim(&self) -> usize;

    /// An iterator over the LineStrings in this MultiLineString
    fn lines(&self) -> MultiLineStringIterator<'_, Self::T, Self::ItemType<'_>, Self> {
        MultiLineStringIterator::new(self, 0, self.num_lines())
    }

    /// The number of lines in this MultiLineString
    fn num_lines(&self) -> usize;

    /// Access to a specified line in this MultiLineString
    /// Will return None if the provided index is out of bounds
    fn line(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i >= self.num_lines() {
            None
        } else {
            unsafe { Some(self.line_unchecked(i)) }
        }
    }

    /// Access to a specified line in this MultiLineString
    ///
    /// # Safety
    ///
    /// Accessing an index out of bounds is UB.
    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_>;
}

impl<T: CoordNum> MultiLineStringTrait for MultiLineString<T> {
    type T = T;
    type ItemType<'a> = &'a LineString<Self::T> where Self: 'a;

    fn dim(&self) -> usize {
        2
    }

    fn num_lines(&self) -> usize {
        self.0.len()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.0.get_unchecked(i)
    }
}

impl<'a, T: CoordNum> MultiLineStringTrait for &'a MultiLineString<T> {
    type T = T;
    type ItemType<'b> = &'a LineString<Self::T> where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn num_lines(&self) -> usize {
        self.0.len()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        self.0.get_unchecked(i)
    }
}
