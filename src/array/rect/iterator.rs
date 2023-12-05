use arrow_buffer::NullBuffer;

use crate::array::RectArray;
use crate::scalar::Rect;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;

/// Iterator of values of a [`RectArray`]
#[derive(Clone, Debug)]
pub struct RectArrayIter<'a> {
    array: &'a RectArray,
    logical_nulls: Option<NullBuffer>,
    current: usize,
    current_end: usize,
}

impl<'a> RectArrayIter<'a> {
    #[inline]
    pub fn new(array: &'a RectArray) -> Self {
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

impl<'a> Iterator for RectArrayIter<'a> {
    type Item = Option<Rect<'a>>;

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
impl<'a> DoubleEndedIterator for RectArrayIter<'a> {
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
impl<'a> ExactSizeIterator for RectArrayIter<'a> {}

impl<'a> RectArray {
    /// Returns an iterator of `Option<Rect>`
    pub fn iter(&'a self) -> RectArrayIter {
        RectArrayIter::new(self)
    }
}
