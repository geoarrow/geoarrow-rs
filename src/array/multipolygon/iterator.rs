use crate::array::MultiPolygonArray;
use crate::scalar::MultiPolygon;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBuffer;

/// Iterator of values of a [`MultiPolygonArray`]
#[derive(Clone, Debug)]
pub struct MultiPolygonArrayIter<'a, O: OffsetSizeTrait> {
    array: &'a MultiPolygonArray<O>,
    logical_nulls: Option<NullBuffer>,
    current: usize,
    current_end: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPolygonArrayIter<'a, O> {
    #[inline]
    pub fn new(array: &'a MultiPolygonArray<O>) -> Self {
        let len = array.len();
        let logical_nulls = array.logical_nulls();
        Self {
            array,
            logical_nulls,
            current: 0,
            current_end: len,
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        self.logical_nulls
            .as_ref()
            .map(|x| x.is_null(idx))
            .unwrap_or_default()
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for MultiPolygonArrayIter<'a, O> {
    type Item = Option<MultiPolygon<'a, O>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(old))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<'a, O: OffsetSizeTrait> DoubleEndedIterator for MultiPolygonArrayIter<'a, O> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<'a, O: OffsetSizeTrait> ExactSizeIterator for MultiPolygonArrayIter<'a, O> {}

impl<'a, O: OffsetSizeTrait> MultiPolygonArray<O> {
    /// Returns an iterator of `Option<MultiPolygon>`
    pub fn iter(&'a self) -> MultiPolygonArrayIter<O> {
        MultiPolygonArrayIter::new(self)
    }
}
