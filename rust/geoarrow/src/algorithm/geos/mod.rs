//! Bindings to the [`geos`] crate for geometry operations.

mod area;
mod buffer;
mod is_empty;
mod is_ring;
mod is_simple;
mod is_valid;
mod length;
mod util;

pub use area::Area;
pub use buffer::Buffer;
pub use is_empty::IsEmpty;
pub use is_ring::IsRing;
pub use is_simple::IsSimple;
pub use is_valid::IsValid;
pub use length::Length;
