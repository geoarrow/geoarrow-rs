use crate::algorithm::broadcasting::{BroadcastablePrimitive, BroadcastableVec};
use crate::algorithm::geo::affine::affine_transform;
use crate::algorithm::geo::{Center, Centroid, TransformOrigin};
use crate::array::GeometryArray;
use crate::error::Result;
use geo::AffineTransform;

pub fn rotate(
    array: &GeometryArray,
    angle: BroadcastablePrimitive<f64>,
    origin: TransformOrigin,
) -> Result<GeometryArray> {
    // TODO: validate lengths between array and angle

    match origin {
        TransformOrigin::Centroid => {
            // compute centroid of all geoms
            let centroids = array.centroid();
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
            let centers = array.center();
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
            // Note: We need to unpack the enum here because otherwise the scalar will iter forever
            let transforms = match angle {
                BroadcastablePrimitive::Scalar(angle) => {
                    BroadcastableVec::Scalar(AffineTransform::rotate(angle, point))
                }
                BroadcastablePrimitive::Array(angle) => {
                    let transforms: Vec<AffineTransform> = angle
                        .values_iter()
                        .map(|angle| AffineTransform::rotate(*angle, point))
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
            };

            affine_transform(array, transforms)
        }
    }
}
