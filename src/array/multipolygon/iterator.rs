use crate::array::MultiPolygonArray;
use crate::scalar::MultiPolygon;
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`MultiPolygonArray`]
#[derive(Clone, Debug)]
pub struct MultiPolygonArrayValuesIter<'a, O: OffsetSizeTrait> {
    array: &'a MultiPolygonArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPolygonArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a MultiPolygonArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for MultiPolygonArrayValuesIter<'a, O> {
    type Item = MultiPolygon<'a, O>;

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

impl<'a, O: OffsetSizeTrait> ExactSizeIterator for MultiPolygonArrayValuesIter<'a, O> {}

unsafe impl<'a, O: OffsetSizeTrait> TrustedLen for MultiPolygonArrayValuesIter<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for MultiPolygonArrayValuesIter<'a, O> {
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

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a MultiPolygonArray<O> {
    type Item = Option<MultiPolygon<'a, O>>;
    type IntoIter =
        ZipValidity<MultiPolygon<'a, O>, MultiPolygonArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> MultiPolygonArray<O> {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<MultiPolygon<'a, O>, MultiPolygonArrayValuesIter<'a, O>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(MultiPolygonArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> MultiPolygonArrayValuesIter<'a, O> {
        MultiPolygonArrayValuesIter::new(self)
    }
}
