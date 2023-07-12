//! Algorithms implemented on GeoArrow arrays using georust/geo algorithms.

mod affine;
mod distance;
mod geodesic_area;
mod simplify;
pub(crate) mod utils;

pub use affine::{affine_transform, rotate, scale, skew, translate, TransformOrigin};
pub use geodesic_area::{geodesic_area_signed, geodesic_area_unsigned, geodesic_perimeter};
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

/// Calculate the length of a planar length of a `LineString`.
pub mod euclidean_length;
pub use euclidean_length::EuclideanLength;

/// Calculate the Geodesic length of a line.
pub mod geodesic_length;
pub use geodesic_length::GeodesicLength;

/// Calculate the Haversine length of a Line.
pub mod haversine_length;
pub use haversine_length::HaversineLength;

/// Rotate a `Geometry` by an angle given in degrees.
pub mod rotate;
pub use rotate::Rotate;

/// Calculate the Vincenty length of a `LineString`.
pub mod vincenty_length;
pub use vincenty_length::VincentyLength;

mod scale;
