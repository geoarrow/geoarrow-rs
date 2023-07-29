//! Contains vectorized algorithms implemented on GeoArrow arrays using [geo] algorithms.

// mod affine;
pub(crate) mod utils;

// pub use affine::{affine_transform, rotate, scale, skew, translate, TransformOrigin};

/// Composable affine operations such as rotate, scale, skew, and translate
pub mod affine_ops;
pub use affine_ops::AffineOps;
pub use geo::AffineTransform;

/// Calculate the area of the surface of geometries.
pub mod area;
pub use area::Area;

/// Calculate the bounding rectangle of geometries.
pub mod bounding_rect;
pub use bounding_rect::BoundingRect;

/// Calculate the center of geometries.
pub mod center;
pub use center::Center;

/// Calculate the centroid of geometries.
pub mod centroid;
pub use centroid::Centroid;

/// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
pub mod chaikin_smoothing;
pub use chaikin_smoothing::ChaikinSmoothing;

/// Calculate the signed approximate geodesic area of geometries.
pub mod chamberlain_duquette_area;
pub use chamberlain_duquette_area::ChamberlainDuquetteArea;

/// Determine whether `Geometry` `A` completely encloses `Geometry` `B`.
pub mod contains;
pub use contains::Contains;

/// Calculate the convex hull of geometries.
pub mod convex_hull;
pub use convex_hull::ConvexHull;

/// Densify linear geometry components
pub mod densify;
pub use densify::Densify;

/// Dimensionality of a geometry and its boundary, based on OGC-SFA.
pub mod dimensions;
pub use dimensions::HasDimensions;

/// Calculate the length of a planar length of a
/// [`LineStringArray`][crate::array::LineStringArray].
pub mod euclidean_length;
pub use euclidean_length::EuclideanLength;

/// Calculate the minimum Euclidean distance between two `Geometries`.
pub mod euclidean_distance;
pub use euclidean_distance::EuclideanDistance;

/// Calculate the Geodesic area and perimeter of polygons.
pub mod geodesic_area;
pub use geodesic_area::GeodesicArea;

/// Calculate the Geodesic length of a line.
pub mod geodesic_length;
pub use geodesic_length::GeodesicLength;

/// Calculate the Haversine length of a Line.
pub mod haversine_length;
pub use haversine_length::HaversineLength;

/// Determine whether `Geometry` `A` intersects `Geometry` `B`.
pub mod intersects;
pub use intersects::Intersects;

/// Interpolate a point along a `LineStringArray`.
pub mod line_interpolate_point;
pub use line_interpolate_point::LineInterpolatePoint;

/// Locate a point along a `LineStringArray`.
pub mod line_locate_point;
pub use line_locate_point::LineLocatePoint;

/// Calculate the minimum rotated rectangle of a `Geometry`.
pub mod minimum_rotated_rect;
pub use minimum_rotated_rect::MinimumRotatedRect;

/// Remove (consecutive) repeated points
pub mod remove_repeated_points;
pub use remove_repeated_points::RemoveRepeatedPoints;

/// Rotate geometries by an angle given in degrees.
pub mod rotate;
pub use rotate::Rotate;

/// Scale geometries up or down by a factor
pub mod scale;
pub use scale::Scale;

/// Simplify geometries using the Ramer-Douglas-Peucker algorithm.
pub mod simplify;
pub use simplify::Simplify;

/// Simplify geometries using the Visvalingam-Whyatt algorithm.
pub mod simplify_vw;
pub use simplify_vw::SimplifyVw;

/// Skew geometries by shearing it at angles along the x and y dimensions
pub mod skew;
pub use skew::Skew;

/// Translate geometries along the given offsets.
pub mod translate;
pub use translate::Translate;

/// Calculate the Vincenty length of a [`LineStringArray`][crate::array::LineStringArray].
pub mod vincenty_length;
pub use vincenty_length::VincentyLength;

/// Determine whether `Geometry` `A` is completely within by `Geometry` `B`.
pub mod within;
pub use within::Within;
