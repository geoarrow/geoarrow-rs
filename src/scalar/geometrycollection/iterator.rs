use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::{Geometry, GeometryCollection};
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`GeometryCollectionArray`]
#[derive(Clone, Debug)]
pub struct GeometryCollectionIterator<'a, O: OffsetSizeTrait> {
    geom: &'a GeometryCollection<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> GeometryCollectionIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a GeometryCollection<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_geometries(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for GeometryCollectionIterator<'a, O> {
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

impl<'a, O: OffsetSizeTrait> ExactSizeIterator for GeometryCollectionIterator<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for GeometryCollectionIterator<'a, O> {
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

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a GeometryCollection<'a, O> {
    type Item = Geometry<'a, O>;
    type IntoIter = GeometryCollectionIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryCollection<'a, O> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> GeometryCollectionIterator<'a, O> {
        GeometryCollectionIterator::new(self)
    }
}
