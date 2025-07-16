#![warn(unused_crate_dependencies)]

mod area;
mod centroid;
mod contains;
mod convex_hull;
mod intersects;
mod relate;
mod simplify;
pub mod util;

pub use area::{signed_area, unsigned_area};
pub use centroid::centroid;
pub use contains::contains;
pub use convex_hull::convex_hull;
pub use intersects::intersects;
pub use relate::relate_boolean;
pub use simplify::simplify;
