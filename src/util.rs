use arrow_array::OffsetSizeTrait;
use arrow_buffer::{BufferBuilder, NullBuffer, NullBufferBuilder, OffsetBuffer};

#[inline]
pub(crate) unsafe fn slice_validity_unchecked(
    validity: &mut Option<NullBuffer>,
    offset: usize,
    length: usize,
) {
    let all_bits_set = validity
        .as_mut()
        .map(|bitmap| {
            bitmap.slice_unchecked(offset, length);
            bitmap.unset_bits() == 0
        })
        .unwrap_or(false);

    if all_bits_set {
        *validity = None
    }
}

pub(crate) fn owned_slice_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    offset: usize,
    length: usize,
) -> OffsetBuffer<O> {
    let mut sliced_offsets = offsets.clone();
    // This is annoying/hard to catch but the implementation of slice is on the _raw offsets_ not
    // the logical values, so we have to add 1 ourselves.
    sliced_offsets.slice(offset, length + 1);

    let mut new_offsets: BufferBuilder<O> = BufferBuilder::new(length);

    for item in sliced_offsets.lengths() {
        new_offsets.try_push_usize(item).unwrap();
    }

    new_offsets.into()
}

pub(crate) fn owned_slice_validity(
    validity: Option<&NullBuffer>,
    offset: usize,
    length: usize,
) -> Option<NullBuffer> {
    if let Some(validity) = validity {
        let mut sliced_validity = validity.clone();
        sliced_validity.slice(offset, length);

        let mut new_bitmap = NullBufferBuilder::with_capacity(length);
        for value in validity {
            new_bitmap.push(value);
        }

        Some(new_bitmap.into())
    } else {
        None
    }
}
