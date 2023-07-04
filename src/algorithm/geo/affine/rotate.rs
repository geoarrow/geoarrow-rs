use crate::algorithm::broadcasting::{BroadcastablePrimitive, BroadcastableVec};
use crate::algorithm::geo::affine::affine_transform;
use crate::algorithm::geo::{center, centroid};
use crate::array::GeometryArray;
use crate::error::Result;
use geo::AffineTransform;

/// Used to express the origin for a given transform. Can be specified either be with reference to
/// the geometry being transformed (Centroid, Center) or some arbitrary point.
///
/// - Centroid: Use the centriod of each geometry in the series as the transform origin.
/// - Center: Use the center of each geometry in the series as the transform origin. The center is
///   defined as the center of the bounding box of the geometry
/// - Point: Define a single point to transform each geometry in the series about.
pub enum TransformOrigin {
    Centroid,
    Center,
    Point(geo::Point),
}

pub fn rotate(
    array: GeometryArray,
    angle: BroadcastablePrimitive<f64>,
    origin: TransformOrigin,
) -> Result<GeometryArray> {
    // TODO: validate lengths between array and angle

    match origin {
        TransformOrigin::Centroid => {
            // compute centroid of all geoms
            let centroids = centroid(&array)?;
            let transforms: Vec<AffineTransform> = centroids
                .values_iter()
                .zip(angle.into_iter())
                .map(|(point, angle)| {
                    let point: geo::Point = point.into();
                    AffineTransform::rotate(angle, point)
                })
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
        TransformOrigin::Center => {
            let centers = center(&array)?;
            let transforms: Vec<AffineTransform> = centers
                .values_iter()
                .zip(angle.into_iter())
                .map(|(point, angle)| {
                    let point: geo::Point = point.into();
                    AffineTransform::rotate(angle, point)
                })
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
        TransformOrigin::Point(point) => {
            // TODO: how does this know how long to iterate if it's just a scalar?
            let transforms: Vec<AffineTransform> = angle
                .into_iter()
                .map(|angle| AffineTransform::rotate(angle, point))
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
    }
}
