//! Operations that are implemented natively in this crate.
//!
//! Where possible, operations on scalars are implemented in terms of [geometry
//! traits](../../geo_traits).

pub mod bounding_rect;
mod cast;
mod concatenate;
mod downcast;
pub(crate) mod eq;
mod rechunk;
mod take;
pub(crate) mod type_id;

pub use cast::Cast;
pub use concatenate::Concatenate;
pub use downcast::Downcast;
pub use rechunk::Rechunk;
pub use take::Take;
