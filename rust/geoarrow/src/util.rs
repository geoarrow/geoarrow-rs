use arrow_array::OffsetSizeTrait;
use arrow_buffer::{NullBuffer, NullBufferBuilder, OffsetBuffer};

use crate::array::offset_builder::OffsetsBuilder;
use crate::array::util::offset_lengths;

pub(crate) fn owned_slice_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    offset: usize,
    length: usize,
) -> OffsetBuffer<O> {
    // TODO: double check but now that we've moved to arrow-rs it looks like this slice adds 1 for
    // us.
    let sliced_offsets = offsets.slice(offset, length);

    let mut new_offsets: OffsetsBuilder<O> = OffsetsBuilder::with_capacity(length);

    for item in offset_lengths(&sliced_offsets) {
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
        let sliced_validity = validity.slice(offset, length);

        let mut new_bitmap = NullBufferBuilder::new(length);
        for value in sliced_validity.into_iter() {
            new_bitmap.append(value);
        }

        new_bitmap.finish()
    } else {
        None
    }
}
