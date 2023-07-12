//! Algorithms implemented on GeoArrow arrays using georust/geo algorithms.

mod affine;
mod convex_hull;
mod distance;
mod envelope;
mod geodesic_area;
mod is_empty;
mod length;
mod simplify;
pub(crate) mod utils;

pub use affine::{affine_transform, rotate, scale, skew, translate, TransformOrigin};
pub use convex_hull::convex_hull;
pub use envelope::envelope;
pub use geodesic_area::{geodesic_area_signed, geodesic_area_unsigned, geodesic_perimeter};
pub use is_empty::is_empty;
pub use length::{euclidean_length, geodesic_length, haversine_length, vincenty_length};
pub use simplify::simplify;

/// Calculate the area of the surface of a `Geometry`.
pub mod area;
pub use area::Area;

/// Calculate the center of a `Geometry`.
pub mod center;
pub use center::Center;

/// Calculate the centroid of a `Geometry`.
pub mod centroid;
pub use centroid::Centroid;

/// Calculate the signed approximate geodesic area of a `Geometry`.
pub mod chamberlain_duquette_area;
pub use chamberlain_duquette_area::ChamberlainDuquetteArea;
