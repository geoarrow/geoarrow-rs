#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

mod area;
mod centroid;
mod contains;
mod convex_hull;
mod distance;
mod interior_point;
mod intersects;
mod minimum_rotated_rect;
mod relate;
mod simplify;
mod simplify_vw;
mod simplify_vw_preserve;
pub mod util;
pub mod validation;

pub use area::{signed_area, unsigned_area};
pub use centroid::centroid;
pub use contains::contains;
pub use convex_hull::convex_hull;
pub use distance::euclidean_distance;
pub use interior_point::interior_point;
pub use intersects::intersects;
pub use minimum_rotated_rect::minimum_rotated_rect;
pub use relate::relate_boolean;
pub use simplify::simplify;
pub use simplify_vw::simplify_vw;
pub use simplify_vw_preserve::simplify_vw_preserve;
