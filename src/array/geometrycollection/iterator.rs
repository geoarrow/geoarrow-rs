use crate::array::GeometryCollectionArray;
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::{Geometry, GeometryCollection};
use crate::GeometryArrayTrait;
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;
use arrow2::types::Offset;

/// Iterator of values of a [`GeometryCollectionArray`]
#[derive(Clone, Debug)]
pub struct GeometryCollectionArrayValuesIter<'a, O: Offset> {
    array: &'a GeometryCollectionArray<O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> GeometryCollectionArrayValuesIter<'a, O> {
    #[inline]
    pub fn new(array: &'a GeometryCollectionArray<O>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, O: Offset> Iterator for GeometryCollectionArrayValuesIter<'a, O> {
    type Item = GeometryCollection<'a, O>;

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

impl<'a, O: Offset> ExactSizeIterator for GeometryCollectionArrayValuesIter<'a, O> {}

unsafe impl<'a, O: Offset> TrustedLen for GeometryCollectionArrayValuesIter<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for GeometryCollectionArrayValuesIter<'a, O> {
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

impl<'a, O: Offset> IntoIterator for &'a GeometryCollectionArray<O> {
    type Item = Option<GeometryCollection<'a, O>>;
    type IntoIter = ZipValidity<
        GeometryCollection<'a, O>,
        GeometryCollectionArrayValuesIter<'a, O>,
        BitmapIter<'a>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> GeometryCollectionArray<O> {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<
        GeometryCollection<'a, O>,
        GeometryCollectionArrayValuesIter<'a, O>,
        BitmapIter<'a>,
    > {
        ZipValidity::new_with_validity(
            GeometryCollectionArrayValuesIter::new(self),
            self.validity(),
        )
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> GeometryCollectionArrayValuesIter<'a, O> {
        GeometryCollectionArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`GeometryCollectionArray`]
#[derive(Clone, Debug)]
pub struct GeometryCollectionIterator<'a, O: Offset> {
    geom: &'a GeometryCollection<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: Offset> GeometryCollectionIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a GeometryCollection<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_geometries(),
        }
    }
}

impl<'a, O: Offset> Iterator for GeometryCollectionIterator<'a, O> {
    type Item = crate::scalar::Geometry<'a, O>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.geometry(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a, O: Offset> ExactSizeIterator for GeometryCollectionIterator<'a, O> {}

unsafe impl<'a, O: Offset> TrustedLen for GeometryCollectionIterator<'a, O> {}

impl<'a, O: Offset> DoubleEndedIterator for GeometryCollectionIterator<'a, O> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.geometry(self.end)
        }
    }
}

impl<'a, O: Offset> IntoIterator for &'a GeometryCollection<'a, O> {
    type Item = Geometry<'a, O>;
    type IntoIter = GeometryCollectionIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> GeometryCollection<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> GeometryCollectionIterator<'a, O> {
        GeometryCollectionIterator::new(self)
    }
}
