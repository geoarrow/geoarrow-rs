#![warn(unused_crate_dependencies)]

mod area;
mod convex_hull;
mod intersects;
mod simplify;

pub use area::{signed_area, unsigned_area};
pub use convex_hull::convex_hull;
pub use intersects::intersects;
pub use simplify::simplify;
