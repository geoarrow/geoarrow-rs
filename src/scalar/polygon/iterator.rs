use crate::geo_traits::PolygonTrait;
use crate::scalar::{LineString, Polygon};
use arrow_array::OffsetSizeTrait;

/// Iterator of values of a [`PolygonArray`](crate::array::PolygonArray)
#[derive(Clone, Debug)]
pub struct PolygonInteriorIterator<'a, O: OffsetSizeTrait> {
    geom: &'a Polygon<'a, O>,
    index: usize,
    end: usize,
}

impl<'a, O: OffsetSizeTrait> PolygonInteriorIterator<'a, O> {
    #[inline]
    pub fn new(geom: &'a Polygon<'a, O>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_interiors(),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for PolygonInteriorIterator<'a, O> {
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

impl<'a, O: OffsetSizeTrait> ExactSizeIterator for PolygonInteriorIterator<'a, O> {}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for PolygonInteriorIterator<'a, O> {
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

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a Polygon<'a, O> {
    type Item = LineString<'a, O>;
    type IntoIter = PolygonInteriorIterator<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: OffsetSizeTrait> Polygon<'a, O> {
    /// Returns an iterator of `LineString`
    pub fn iter(&'a self) -> PolygonInteriorIterator<'a, O> {
        PolygonInteriorIterator::new(self)
    }
}
