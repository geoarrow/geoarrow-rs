use crate::array::MultiLineStringArray;
use crate::scalar::MultiLineString;
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`MultiLineStringArray`]
#[derive(Clone, Debug)]
pub struct MultiLineStringArrayValuesIter<'a, O: OffsetSizeTrait> {
    array: &'a MultiLineStringArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> MultiLineStringArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a MultiLineStringArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for MultiLineStringArrayValuesIter<'a, O> {
    type Item = MultiLineString<'a, O>;

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

impl<'a, O: OffsetSizeTrait> ExactSizeIterator for MultiLineStringArrayValuesIter<'a, O> {}

unsafe impl<'a, O: OffsetSizeTrait> TrustedLen for MultiLineStringArrayValuesIter<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for MultiLineStringArrayValuesIter<'a, O> {
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

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a MultiLineStringArray<O> {
    type Item = Option<MultiLineString<'a, O>>;
    type IntoIter =
        ZipValidity<MultiLineString<'a, O>, MultiLineStringArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> MultiLineStringArray<O> {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<MultiLineString<'a, O>, MultiLineStringArrayValuesIter<'a, O>, BitmapIter<'a>>
    {
        ZipValidity::new_with_validity(MultiLineStringArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> MultiLineStringArrayValuesIter<'a, O> {
        MultiLineStringArrayValuesIter::new(self)
    }
}
