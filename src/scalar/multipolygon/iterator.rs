use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::{MultiPolygon, Polygon};
use arrow2::trusted_len::TrustedLen;
use arrow2::types::Offset;

/// Iterator of values of a [`MultiPolygonArray`]
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
