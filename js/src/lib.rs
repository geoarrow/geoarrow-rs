pub mod array;
pub mod broadcasting;
pub mod error;
#[cfg(feature = "geodesy")]
pub mod reproject;
pub mod transform_origin;
pub mod utils;

pub use transform_origin::TransformOrigin;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;
