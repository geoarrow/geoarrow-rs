use crate::array::MultiLineStringArray;
use crate::geo_traits::MultiLineStringTrait;
use crate::scalar::{LineString, MultiLineString};
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow2::types::Offset;

/// Iterator of values of a [`MultiLineStringArray`]
#[derive(Clone, Debug)]
pub struct MultiLineStringArrayValuesIter<'a, O: Offset> {
    array: &'a MultiLineStringArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> MultiLineStringArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a MultiLineStringArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for MultiLineStringArrayValuesIter<'a, O> {
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

impl<'a, O: Offset> ExactSizeIterator for MultiLineStringArrayValuesIter<'a, O> {}

unsafe impl<'a, O: Offset> TrustedLen for MultiLineStringArrayValuesIter<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for MultiLineStringArrayValuesIter<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a MultiLineStringArray<O> {
    type Item = Option<MultiLineString<'a, O>>;
    type IntoIter =
        ZipValidity<MultiLineString<'a, O>, MultiLineStringArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> MultiLineStringArray<O> {
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

/// Iterator of values of a [`MultiLineStringArray`]
#[derive(Clone, Debug)]
pub struct MultiLineStringIterator<'a, O: Offset> {
    geom: &'a MultiLineString<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> MultiLineStringIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a MultiLineString<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_lines(),
        }
    }
}

impl<'a, O: Offset> Iterator for MultiLineStringIterator<'a, O> {
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

impl<'a, O: Offset> ExactSizeIterator for MultiLineStringIterator<'a, O> {}

unsafe impl<'a, O: Offset> TrustedLen for MultiLineStringIterator<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for MultiLineStringIterator<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a MultiLineString<'a, O> {
    type Item = LineString<'a, O>;
    type IntoIter = MultiLineStringIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> MultiLineString<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> MultiLineStringIterator<'a, O> {
        MultiLineStringIterator::new(self)
    }
}
