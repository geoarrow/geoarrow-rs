//! Operations that are implemented natively in this crate.
//!
//! Where possible, operations on scalars are implemented in terms of [geometry
//! traits](../../geo_traits).

mod binary;
pub mod bounding_rect;
mod cast;
mod concatenate;
mod downcast;
pub(crate) mod eq;
mod explode;
mod map_chunks;
mod map_coords;
mod rechunk;
mod take;
mod total_bounds;
pub(crate) mod type_id;
mod unary;

pub use binary::Binary;
pub use cast::Cast;
pub use concatenate::Concatenate;
pub use downcast::Downcast;
pub use explode::Explode;
pub use map_chunks::MapChunks;
pub use map_coords::{MapCoords, MapCoordsChunked};
pub use rechunk::Rechunk;
pub use take::Take;
pub use total_bounds::TotalBounds;
pub use unary::Unary;
