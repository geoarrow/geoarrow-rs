use crate::array::PolygonArray;
use crate::geo_traits::PolygonTrait;
use crate::scalar::{LineString, Polygon};
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow2::types::Offset;

/// Iterator of values of a [`PolygonArray`]
#[derive(Clone, Debug)]
pub struct PolygonArrayValuesIter<'a, O: Offset> {
    array: &'a PolygonArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> PolygonArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a PolygonArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for PolygonArrayValuesIter<'a, O> {
    type Item = Polygon<'a, O>;

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

unsafe impl<'a, O: Offset> TrustedLen for PolygonArrayValuesIter<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for PolygonArrayValuesIter<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a PolygonArray<O> {
    type Item = Option<Polygon<'a, O>>;
    type IntoIter = ZipValidity<Polygon<'a, O>, PolygonArrayValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> PolygonArray<O> {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<Polygon<'a, O>, PolygonArrayValuesIter<'a, O>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(PolygonArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> PolygonArrayValuesIter<'a, O> {
        PolygonArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct PolygonInteriorIterator<'a, O: Offset> {
    geom: &'a Polygon<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> PolygonInteriorIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a Polygon<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_interiors(),
        }
    }
}

impl<'a, O: Offset> Iterator for PolygonInteriorIterator<'a, O> {
    type Item = crate::scalar::LineString<'a, O>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.interior(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a, O: Offset> ExactSizeIterator for PolygonInteriorIterator<'a, O> {
    // fn len(&self) -> usize {
    //     self.end
    // }
}

unsafe impl<'a, O: Offset> TrustedLen for PolygonInteriorIterator<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for PolygonInteriorIterator<'a, O> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.interior(self.end)
        }
    }
}

impl<'a, O: Offset> IntoIterator for &'a Polygon<'a, O> {
    type Item = LineString<'a, O>;
    type IntoIter = PolygonInteriorIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> Polygon<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> PolygonInteriorIterator<'a, O> {
        PolygonInteriorIterator::new(self)
    }
}
