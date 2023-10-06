use crate::array::MixedGeometryArray;
use crate::scalar::Geometry;
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`MixedGeometryArray`]
#[derive(Clone, Debug)]
pub struct MixedGeometryArrayValuesIter<'a, O: OffsetSizeTrait> {
    array: &'a MixedGeometryArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> MixedGeometryArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a MixedGeometryArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for MixedGeometryArrayValuesIter<'a, O> {
    type Item = Geometry<'a, O>;

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

unsafe impl<'a, O: OffsetSizeTrait> TrustedLen for MixedGeometryArrayValuesIter<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for MixedGeometryArrayValuesIter<'a, O> {
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

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a MixedGeometryArray<O> {
    type Item = Option<Geometry<'a, O>>;
    type IntoIter =
        ZipValidity<Geometry<'a, O>, MixedGeometryArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> MixedGeometryArray<O> {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<Geometry<'a, O>, MixedGeometryArrayValuesIter<'a, O>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(MixedGeometryArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> MixedGeometryArrayValuesIter<'a, O> {
        MixedGeometryArrayValuesIter::new(self)
    }
}
