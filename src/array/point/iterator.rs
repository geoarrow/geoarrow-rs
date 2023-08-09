use crate::array::{PointArray, CoordBuffer};
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct PointArrayValuesIter<'a, C: CoordBuffer> {
    array: &'a PointArray<C>,
    index: usize,
    end: usize,
}

impl<'a> PointArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a PointArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for PointArrayValuesIter<'a> {
    type Item = crate::scalar::Point<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(self.array.value(old))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a> ExactSizeIterator for PointArrayValuesIter<'a> {}

unsafe impl<'a> TrustedLen for PointArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for PointArrayValuesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<'a> IntoIterator for &'a PointArray {
    type Item = Option<crate::scalar::Point<'a>>;
    type IntoIter = ZipValidity<crate::scalar::Point<'a>, PointArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> PointArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<crate::scalar::Point<'a>, PointArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(PointArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> PointArrayValuesIter<'a> {
        PointArrayValuesIter::new(self)
    }
}
