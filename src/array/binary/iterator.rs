use crate::array::WKBArray;
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`WKBArray`]
#[derive(Clone, Debug)]
pub struct WKBArrayValuesIter<'a, O: OffsetSizeTrait> {
    array: &'a WKBArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> WKBArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a WKBArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for WKBArrayValuesIter<'a, O> {
    type Item = crate::scalar::WKB<'a, O>;

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

unsafe impl<'a, O: OffsetSizeTrait> TrustedLen for WKBArrayValuesIter<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for WKBArrayValuesIter<'a, O> {
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

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a WKBArray<O> {
    type Item = Option<crate::scalar::WKB<'a, O>>;
    type IntoIter =
        ZipValidity<crate::scalar::WKB<'a, O>, WKBArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> WKBArray<O> {
    /// Returns an iterator of `Option<WKB>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<crate::scalar::WKB<'a, O>, WKBArrayValuesIter<'a, O>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(WKBArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `WKB`
    pub fn values_iter(&'a self) -> WKBArrayValuesIter<'a, O> {
        WKBArrayValuesIter::new(self)
    }
}
