use crate::geo_traits::MultiLineStringTrait;
use crate::scalar::{LineString, MultiLineString};
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`MultiLineStringArray`]
#[derive(Clone, Debug)]
pub struct MultiLineStringIterator<'a, O: OffsetSizeTrait> {
    geom: &'a MultiLineString<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> MultiLineStringIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a MultiLineString<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_lines(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for MultiLineStringIterator<'a, O> {
    type Item = crate::scalar::LineString<'a, O>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.line(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a, O: OffsetSizeTrait> ExactSizeIterator for MultiLineStringIterator<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for MultiLineStringIterator<'a, O> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.line(self.end)
        }
    }
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a MultiLineString<'a, O> {
    type Item = LineString<'a, O>;
    type IntoIter = MultiLineStringIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> MultiLineString<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> MultiLineStringIterator<'a, O> {
        MultiLineStringIterator::new(self)
    }
}
