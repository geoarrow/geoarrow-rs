//! Algorithms implemented on GeoArrow arrays using georust/geo algorithms.

mod affine;
mod distance;
mod geodesic_area;
mod length;
mod simplify;
pub(crate) mod utils;

pub use affine::{affine_transform, rotate, scale, skew, translate, TransformOrigin};
pub use geodesic_area::{geodesic_area_signed, geodesic_area_unsigned, geodesic_perimeter};
pub use length::{euclidean_length, geodesic_length, haversine_length, vincenty_length};
pub use simplify::simplify;

/// Composable affine operations such as rotate, scale, skew, and translate
pub mod affine_ops;
pub use affine_ops::AffineOps;
pub use geo::AffineTransform;

/// Calculate the area of the surface of a `Geometry`.
pub mod area;
pub use area::Area;

/// Calculate the bounding rectangle of a `Geometry`.
pub mod bounding_rect;
pub use bounding_rect::BoundingRect;

/// Calculate the center of a `Geometry`.
pub mod center;
pub use center::Center;

/// Calculate the centroid of a `Geometry`.
pub mod centroid;
pub use centroid::Centroid;

/// Calculate the signed approximate geodesic area of a `Geometry`.
pub mod chamberlain_duquette_area;
pub use chamberlain_duquette_area::ChamberlainDuquetteArea;

/// Calculate the convex hull of a `Geometry`.
pub mod convex_hull;
pub use convex_hull::ConvexHull;

/// Dimensionality of a geometry and its boundary, based on OGC-SFA.
pub mod dimensions;
pub use dimensions::HasDimensions;
