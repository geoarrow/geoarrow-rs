#![warn(unused_crate_dependencies)]

mod area;
mod convex_hull;
mod simplify;
mod intersects;

pub use area::{signed_area, unsigned_area};
pub use convex_hull::convex_hull;
pub use simplify::simplify;
pub use intersects::intersects;
