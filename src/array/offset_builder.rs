//! Contains the declaration of [`Offset`]
use std::hint::unreachable_unchecked;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

// use crate::buffer::Buffer;
use crate::error::GeoArrowError as Error;

/// A wrapper type of [`Vec<O>`] representing the invariants of Arrow's offsets.
/// It is guaranteed to (sound to assume that):
/// * every element is `>= 0`
/// * element at position `i` is >= than element at position `i-1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsBuilder<O: OffsetSizeTrait>(Vec<O>);

impl<O: OffsetSizeTrait> Default for OffsetsBuilder<O> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// impl<O: OffsetSizeTrait> TryFrom<Vec<O>> for Offsets<O> {
//     type Error = Error;

//     #[inline]
//     fn try_from(offsets: Vec<O>) -> Result<Self, Self::Error> {
//         try_check_offsets(&offsets)?;
//         Ok(Self(offsets))
//     }
// }

impl<O: OffsetSizeTrait> OffsetsBuilder<O> {
    /// Returns an empty [`Offsets`] (i.e. with a single element, the zero)
    #[inline]
    pub fn new() -> Self {
        Self(vec![O::zero()])
    }

    /// Returns an [`Offsets`] whose all lengths are zero.
    #[inline]
    pub fn new_zeroed(length: usize) -> Self {
        Self(vec![O::zero(); length + 1])
    }

    /// Returns an [`Offsets`] whose all lengths are all 1.
    ///
    /// This is useful for casting from a PointArray to a MultiPointArray where you need to
    /// create a `geom_offsets` buffer where every element has length 1.
    #[inline]
    pub fn new_ones(length: usize) -> Result<Self, Error> {
        // Overflow check
        O::from_usize(length + 1).ok_or(Error::Overflow)?;

        Ok(Self(
            (0..length + 1).map(|x| O::from_usize(x).unwrap()).collect(),
        ))
    }

    /// Creates a new [`Offsets`] from an iterator of lengths
    #[inline]
    pub fn try_from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Result<Self, Error> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut offsets = Self::with_capacity(lower);
        for item in iterator {
            offsets.try_push_usize(item)?
        }
        Ok(offsets)
    }

    /// Returns a new [`Offsets`] with a capacity, allocating at least `capacity + 1` entries.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(O::zero());
        Self(offsets)
    }

    /// Returns the capacity of [`Offsets`].
    pub fn capacity(&self) -> usize {
        self.0.capacity() - 1
    }

    /// Reserves `additional` entries.
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Reserves exactly `additional` entries.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.0.reserve_exact(additional);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// Pushes a new element with a given length.
    /// # Error
    /// This function errors iff the new last item is larger than what `O` supports.
    /// # Panic
    /// This function asserts that `length > 0`.
    #[inline]
    pub fn try_push(&mut self, length: O) -> Result<(), Error> {
        let old_length = self.last();
        assert!(length >= O::zero());
        // let new_length = old_length.checked_add(&length).ok_or(Error::Overflow)?;
        let new_length = *old_length + length;
        self.0.push(new_length);
        Ok(())
    }

    /// Pushes a new element with a given length.
    /// # Error
    /// This function errors iff the new last item is larger than what `O` supports.
    /// # Implementation
    /// This function:
    /// * checks that this length does not overflow
    #[inline]
    pub fn try_push_usize(&mut self, length: usize) -> Result<(), Error> {
        let length = O::from_usize(length).ok_or(Error::Overflow)?;

        let old_length = self.last();
        // let new_length = old_length.checked_add(&length).ok_or(Error::Overflow)?;
        let new_length = *old_length + length;
        self.0.push(new_length);
        Ok(())
    }

    /// Returns [`Offsets`] assuming that `offsets` fulfills its invariants
    /// # Safety
    /// This is safe iff the invariants of this struct are guaranteed in `offsets`.
    #[inline]
    pub unsafe fn new_unchecked(offsets: Vec<O>) -> Self {
        Self(offsets)
    }

    /// Returns the last offset of this container.
    #[inline]
    pub fn last(&self) -> &O {
        match self.0.last() {
            Some(element) => element,
            None => unsafe { unreachable_unchecked() },
        }
    }

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Panic
    /// This function panics iff `index >= self.len()`
    #[inline]
    pub fn start_end(&self, index: usize) -> (usize, usize) {
        // soundness: the invariant of the function
        assert!(index < self.len_proxy());
        unsafe { self.start_end_unchecked(index) }
    }

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Safety
    /// `index` must be `< self.len()`
    #[inline]
    pub unsafe fn start_end_unchecked(&self, index: usize) -> (usize, usize) {
        // soundness: the invariant of the function
        let start = self.0.get_unchecked(index).to_usize().unwrap();
        let end = self.0.get_unchecked(index + 1).to_usize().unwrap();
        (start, end)
    }

    /// Returns the length an array with these offsets would be.
    #[inline]
    pub fn len_proxy(&self) -> usize {
        self.0.len() - 1
    }

    #[inline]
    /// Returns the number of offsets in this container.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the byte slice stored in this buffer
    #[inline]
    pub fn as_slice(&self) -> &[O] {
        self.0.as_slice()
    }

    /// Pops the last element
    #[inline]
    pub fn pop(&mut self) -> Option<O> {
        if self.len_proxy() == 0 {
            None
        } else {
            self.0.pop()
        }
    }

    /// Extends itself with `additional` elements equal to the last offset.
    /// This is useful to extend offsets with empty values, e.g. for null slots.
    #[inline]
    pub fn extend_constant(&mut self, additional: usize) {
        let offset = *self.last();
        if additional == 1 {
            self.0.push(offset)
        } else {
            self.0.resize(self.len() + additional, offset)
        }
    }

    /// Try to create a new [`Offsets`] from a sequence of `lengths`
    /// # Errors
    /// This function errors iff this operation overflows for the maximum value of `O`.
    #[inline]
    pub fn try_from_lengths<I: Iterator<Item = usize>>(lengths: I) -> Result<Self, Error> {
        let mut self_ = Self::with_capacity(lengths.size_hint().0);
        self_.try_extend_from_lengths(lengths)?;
        Ok(self_)
    }

    /// Try extend from an iterator of lengths
    /// # Errors
    /// This function errors iff this operation overflows for the maximum value of `O`.
    #[inline]
    pub fn try_extend_from_lengths<I: Iterator<Item = usize>>(
        &mut self,
        lengths: I,
    ) -> Result<(), Error> {
        let mut total_length = 0;
        let mut offset = *self.last();
        let original_offset = offset.to_usize().unwrap();

        let lengths = lengths.map(|length| {
            total_length += length;
            O::from_usize(length).unwrap()
        });

        let offsets = lengths.map(|length| {
            offset += length; // this may overflow, checked below
            offset
        });
        self.0.extend(offsets);

        let last_offset = original_offset
            .checked_add(total_length)
            .ok_or(Error::Overflow)?;
        O::from_usize(last_offset).ok_or(Error::Overflow)?;
        Ok(())
    }

    /// Extends itself from another [`Offsets`]
    /// # Errors
    /// This function errors iff this operation overflows for the maximum value of `O`.
    pub fn try_extend_from_self(&mut self, other: &Self) -> Result<(), Error> {
        let mut length = *self.last();
        let other_length = *other.last();
        // check if the operation would overflow
        // length.checked_add(&other_length).ok_or(Error::Overflow)?;
        length += other_length;

        let lengths = other.as_slice().windows(2).map(|w| w[1] - w[0]);
        let offsets = lengths.map(|new_length| {
            length += new_length;
            length
        });
        self.0.extend(offsets);
        Ok(())
    }

    /// Returns the inner [`Vec`].
    #[inline]
    pub fn into_inner(self) -> Vec<O> {
        self.0
    }

    pub fn finish(self) -> OffsetBuffer<O> {
        self.into()
    }
}

// /// Checks that `offsets` is monotonically increasing.
// fn try_check_offsets<O: OffsetSizeTrait>(offsets: &[O]) -> Result<(), Error> {
//     // this code is carefully constructed to auto-vectorize, don't change naively!
//     match offsets.first() {
//         None => Err(Error::oos("offsets must have at least one element")),
//         Some(first) => {
//             if *first < O::zero() {
//                 return Err(Error::oos("offsets must be larger than 0"));
//             }
//             let mut previous = *first;
//             let mut any_invalid = false;

//             // This loop will auto-vectorize because there is not any break,
//             // an invalid value will be returned once the whole offsets buffer is processed.
//             for offset in offsets {
//                 if previous > *offset {
//                     any_invalid = true
//                 }
//                 previous = *offset;
//             }

//             if any_invalid {
//                 Err(Error::oos("offsets must be monotonically increasing"))
//             } else {
//                 Ok(())
//             }
//         }
//     }
// }

impl From<OffsetsBuilder<i32>> for OffsetsBuilder<i64> {
    fn from(offsets: OffsetsBuilder<i32>) -> Self {
        // this conversion is lossless and uphelds all invariants
        Self(
            offsets
                .as_slice()
                .iter()
                .map(|x| *x as i64)
                .collect::<Vec<_>>(),
        )
    }
}

impl TryFrom<OffsetsBuilder<i64>> for OffsetsBuilder<i32> {
    type Error = Error;

    fn try_from(offsets: OffsetsBuilder<i64>) -> Result<Self, Self::Error> {
        i32::try_from(*offsets.last()).map_err(|_| Error::Overflow)?;

        // this conversion is lossless and uphelds all invariants
        Ok(Self(
            offsets
                .as_slice()
                .iter()
                .map(|x| *x as i32)
                .collect::<Vec<_>>(),
        ))
    }
}

impl<O: OffsetSizeTrait> From<OffsetsBuilder<O>> for OffsetBuffer<O> {
    fn from(value: OffsetsBuilder<O>) -> Self {
        OffsetBuffer::new(value.0.into())
    }
}
