use arrow2::bitmap::Bitmap;

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
