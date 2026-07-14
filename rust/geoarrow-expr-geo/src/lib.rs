#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

mod affine;
mod area;
mod bool_ops;
mod bounding_rect;
mod buffer;
mod centroid;
mod chamberlain_duquette_area;
mod contains;
mod convex_hull;
mod densify;
mod dim_geom;
mod dimensions;
mod distance;
mod interior_point;
mod intersects;
mod length;
mod line_locate_point;
mod minimum_rotated_rect;
mod relate;
mod simplify;
#[cfg(test)]
mod test_util;
pub mod util;
pub mod validation;

pub use affine::{rotate, scale, skew, translate};
pub use area::{signed_area, unsigned_area};
pub use bool_ops::{difference, intersection, union, xor};
pub use bounding_rect::bounding_rect;
pub use buffer::buffer;
pub use centroid::centroid;
pub use chamberlain_duquette_area::{
    chamberlain_duquette_signed_area, chamberlain_duquette_unsigned_area,
};
pub use contains::contains;
pub use convex_hull::convex_hull;
pub use densify::densify;
pub use dimensions::{dimensions, is_empty};
pub use distance::{euclidean_distance, frechet_distance, hausdorff_distance};
pub use interior_point::interior_point;
pub use intersects::intersects;
pub use length::{euclidean_length, geodesic_length, haversine_length, rhumb_length};
pub use line_locate_point::line_locate_point;
pub use minimum_rotated_rect::minimum_rotated_rect;
pub use relate::relate_boolean;
pub use simplify::{simplify, simplify_vw, simplify_vw_preserve};
