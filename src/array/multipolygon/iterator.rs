use crate::array::MultiPolygonArray;
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::{MultiPolygon, Polygon};
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow2::types::Offset;

/// Iterator of values of a [`MultiPolygonArray`]
#[derive(Clone, Debug)]
pub struct MultiPolygonArrayValuesIter<'a, O: Offset> {
    array: &'a MultiPolygonArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> MultiPolygonArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a MultiPolygonArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for MultiPolygonArrayValuesIter<'a, O> {
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

impl<'a, O: Offset> ExactSizeIterator for MultiPolygonArrayValuesIter<'a, O> {}

unsafe impl<'a, O: Offset> TrustedLen for MultiPolygonArrayValuesIter<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for MultiPolygonArrayValuesIter<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a MultiPolygonArray<O> {
    type Item = Option<MultiPolygon<'a, O>>;
    type IntoIter =
        ZipValidity<MultiPolygon<'a, O>, MultiPolygonArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> MultiPolygonArray<O> {
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

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct MultiPolygonIterator<'a, O: Offset> {
    geom: &'a MultiPolygon<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> MultiPolygonIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a MultiPolygon<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_polygons(),
        }
    }
}

impl<'a, O: Offset> Iterator for MultiPolygonIterator<'a, O> {
    type Item = crate::scalar::Polygon<'a, O>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.polygon(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a, O: Offset> ExactSizeIterator for MultiPolygonIterator<'a, O> {}

unsafe impl<'a, O: Offset> TrustedLen for MultiPolygonIterator<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for MultiPolygonIterator<'a, O> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.polygon(self.end)
        }
    }
}

impl<'a, O: Offset> IntoIterator for &'a MultiPolygon<'a, O> {
    type Item = Polygon<'a, O>;
    type IntoIter = MultiPolygonIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> MultiPolygon<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> MultiPolygonIterator<'a, O> {
        MultiPolygonIterator::new(self)
    }
}
