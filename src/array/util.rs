use arrow_array::OffsetSizeTrait;
use arrow_buffer::BufferBuilder;

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
