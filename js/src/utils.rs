use arrow2::offset::OffsetsBuffer;

pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Convert vec to OffsetsBuffer
pub fn vec_to_offsets(v: Vec<i32>) -> OffsetsBuffer<i64> {
    let offsets = unsafe { OffsetsBuffer::new_unchecked(v.into()) };
    (&offsets).into()
}
