use crate::geo_traits::LineStringTrait;
use crate::scalar::{LineString, Point};
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`LineStringArray`](crate::array::LineStringArray)
#[derive(Clone, Debug)]
pub struct LineStringIterator<'a, O: OffsetSizeTrait> {
    geom: &'a LineString<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> LineStringIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a LineString<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_coords(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for LineStringIterator<'a, O> {
    type Item = crate::scalar::Point<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.coord(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a, O: OffsetSizeTrait> ExactSizeIterator for LineStringIterator<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for LineStringIterator<'a, O> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.coord(self.end)
        }
    }
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a LineString<'a, O> {
    type Item = Point<'a>;
    type IntoIter = LineStringIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> LineString<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> LineStringIterator<'a, O> {
        LineStringIterator::new(self)
    }
}
