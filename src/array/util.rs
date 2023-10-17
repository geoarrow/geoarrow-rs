use arrow_array::OffsetSizeTrait;
use arrow_buffer::{BufferBuilder, OffsetBuffer};

pub(crate) fn extend_offsets_constant<O: OffsetSizeTrait>(buffer: &mut BufferBuilder<O>) {}

/// Pushes a new element with a given length.
/// # Error
/// This function errors iff the new last item is larger than what `O` supports.
/// # Implementation
/// This function:
/// * checks that this length does not overflow
pub(crate) fn try_push_offsets_usize<O: OffsetSizeTrait>(
    buffer: &mut BufferBuilder<O>,
    length: usize,
) {
    let val: O = length.try_into().unwrap();
    todo!()
}

/// Returns the last offset of this container.
#[inline]
pub(crate) fn last_offset<O: OffsetSizeTrait>(buffer: &BufferBuilder<O>) -> O {
    buffer.as_slice()[buffer.len() - 1]
}

/// Offsets utils that I miss from arrow2
pub(crate) trait OffsetBufferUtils<O: OffsetSizeTrait> {
    /// Returns the length an array with these offsets would be.
    fn len_proxy(&self) -> usize;

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Panic
    /// This function panics iff `index >= self.len()`
    fn start_end(&self, index: usize) -> (usize, usize);

    /// Returns the last offset.
    fn last(&self) -> &O;
}

impl<O: OffsetSizeTrait> OffsetBufferUtils<O> for OffsetBuffer<O> {
    /// Returns the length an array with these offsets would be.
    #[inline]
    fn len_proxy(&self) -> usize {
        self.len() - 1
    }

    /// Returns a range (start, end) corresponding to the position `index`
    ///
    /// # Panic
    ///
    /// Panics iff `index >= self.len()`
    #[inline]
    fn start_end(&self, index: usize) -> (usize, usize) {
        assert!(index < self.len_proxy());
        let start = self[index].to_usize().unwrap();
        let end = self[index + 1].to_usize().unwrap();
        (start, end)
    }

    /// Returns the last offset.
    #[inline]
    fn last(&self) -> &O {
        self.last()
    }
}
