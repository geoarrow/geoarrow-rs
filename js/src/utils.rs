use arrow2::offset::OffsetsBuffer;
#[cfg(feature = "console_error_panic_hook")]
use wasm_bindgen::prelude::*;

#[cfg(feature = "console_error_panic_hook")]
#[wasm_bindgen]
pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    console_error_panic_hook::set_once();
}

/// Convert vec to OffsetsBuffer
pub fn vec_to_offsets(v: Vec<i32>) -> OffsetsBuffer<i64> {
    let offsets = unsafe { OffsetsBuffer::new_unchecked(v.into()) };
    (&offsets).into()
}

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[cfg(target_arch = "wasm32")]
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        println!("LOG - {}", format!( $( $t )* ));
    }
}
