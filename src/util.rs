use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::{Offsets, OffsetsBuffer};
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

pub(crate) fn owned_slice_offsets<O: Offset>(
    offsets: &OffsetsBuffer<O>,
    offset: usize,
    length: usize,
) -> OffsetsBuffer<O> {
    let mut sliced_offsets = offsets.clone();
    // This is annoying/hard to catch but the implementation of slice is on the _raw offsets_ not
    // the logical values, so we have to add 1 ourselves.
    sliced_offsets.slice(offset, length + 1);

    let mut new_offsets: Offsets<O> = Offsets::with_capacity(length);

    for item in sliced_offsets.lengths() {
        dbg!(item);
        new_offsets.try_push_usize(item).unwrap();
    }

    new_offsets.into()
}

pub(crate) fn owned_slice_validity(
    validity: Option<&Bitmap>,
    offset: usize,
    length: usize,
) -> Option<Bitmap> {
    if let Some(validity) = validity {
        let mut sliced_validity = validity.clone();
        sliced_validity.slice(offset, length);

        let mut new_bitmap = MutableBitmap::with_capacity(length);
        for value in validity {
            new_bitmap.push(value);
        }

        Some(new_bitmap.into())
    } else {
        None
    }
}
