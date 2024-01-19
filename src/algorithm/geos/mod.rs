//! Bindings to the [`geos`] crate for geometry operations.

mod area;
mod buffer;
mod is_ring;
mod is_valid;
mod length;

pub use area::Area;
pub use buffer::Buffer;
pub use is_ring::IsRing;
pub use is_valid::IsValid;
pub use length::Length;
