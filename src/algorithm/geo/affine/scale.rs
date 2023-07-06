use crate::algorithm::broadcasting::{BroadcastablePrimitive, BroadcastableVec};
use crate::algorithm::geo::affine::affine_transform;
use crate::algorithm::geo::affine::TransformOrigin;
use crate::algorithm::geo::{center, centroid};
use crate::array::GeometryArray;
use crate::error::Result;
use geo::AffineTransform;

pub fn scale(
    array: &GeometryArray,
    xfact: BroadcastablePrimitive<f64>,
    yfact: BroadcastablePrimitive<f64>,
    origin: TransformOrigin,
) -> Result<GeometryArray> {
    // TODO: validate lengths between array and angle

    match origin {
        TransformOrigin::Centroid => {
            // compute centroid of all geoms
            let centroids = centroid(array)?;
            let transforms: Vec<AffineTransform> = centroids
                .values_iter()
                .zip(xfact.into_iter())
                .zip(yfact.into_iter())
                .map(|((point, xfact), yfact)| {
                    let point: geo::Point = point.into();
                    AffineTransform::scale(xfact, yfact, point)
                })
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
        TransformOrigin::Center => {
            let centers = center(array)?;
            let transforms: Vec<AffineTransform> = centers
                .values_iter()
                .zip(xfact.into_iter())
                .zip(yfact.into_iter())
                .map(|((point, xfact), yfact)| {
                    let point: geo::Point = point.into();
                    AffineTransform::scale(xfact, yfact, point)
                })
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
        TransformOrigin::Point(point) => {
            // Note: We need to unpack the enum here because otherwise the scalar will iter forever
            let transforms = match (xfact, yfact) {
                (BroadcastablePrimitive::Scalar(xfact), BroadcastablePrimitive::Scalar(yfact)) => {
                    BroadcastableVec::Scalar(AffineTransform::scale(xfact, yfact, point))
                }
                (BroadcastablePrimitive::Scalar(xfact), BroadcastablePrimitive::Array(yfact)) => {
                    let transforms: Vec<AffineTransform> = yfact
                        .values_iter()
                        .map(|yfact| AffineTransform::scale(xfact, *yfact, point))
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
                (BroadcastablePrimitive::Array(xfact), BroadcastablePrimitive::Scalar(yfact)) => {
                    let transforms: Vec<AffineTransform> = xfact
                        .values_iter()
                        .map(|xfact| AffineTransform::scale(*xfact, yfact, point))
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
                (BroadcastablePrimitive::Array(xfact), BroadcastablePrimitive::Array(yfact)) => {
                    let transforms: Vec<AffineTransform> = xfact
                        .values_iter()
                        .zip(yfact.values_iter())
                        .map(|(xfact, yfact)| AffineTransform::scale(*xfact, *yfact, point))
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
            };

            affine_transform(array, transforms)
        }
    }
}
