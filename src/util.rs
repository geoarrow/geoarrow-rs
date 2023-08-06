use arrow2::bitmap::Bitmap;
use arrow2::offset::{OffsetsBuffer, Offsets};
use arrow2::types::Offset;

#[inline]
pub(crate) unsafe fn slice_validity_unchecked(
    validity: &mut Option<Bitmap>,
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

pub fn owned_slice_offsets<O: Offset>(
    offsets: &OffsetsBuffer<O>,
    offset: usize,
    length: usize,
) -> OffsetsBuffer<O> {
    let mut cloned_offsets = offsets.clone();
    cloned_offsets.slice(offset, length);

    let first_offset =

    cloned_offsets.i
    let x = &*cloned_offsets;

    // cloned_offsets.it
    let sliced_offsets = Offsets::with_capacity(length);
    sliced_offsets.try_push(length)

    sliced_offsets.into()
    // let new_
    // let x = offsets.slice(offset, length);

}
