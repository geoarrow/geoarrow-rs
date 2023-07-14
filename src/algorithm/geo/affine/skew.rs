use crate::algorithm::broadcasting::{BroadcastablePrimitive, BroadcastableVec};
use crate::algorithm::geo::affine::affine_transform;
use crate::algorithm::geo::{Center, Centroid, TransformOrigin};
use crate::array::GeometryArray;
use crate::error::Result;
use geo::AffineTransform;

pub fn skew(
    array: &GeometryArray,
    x_degrees: BroadcastablePrimitive<f64>,
    y_degrees: BroadcastablePrimitive<f64>,
    origin: TransformOrigin,
) -> Result<GeometryArray> {
    // TODO: validate lengths between array and angle

    match origin {
        TransformOrigin::Centroid => {
            // compute centroid of all geoms
            let centroids = array.centroid();
            let transforms: Vec<AffineTransform> = centroids
                .values_iter()
                .zip(x_degrees.into_iter())
                .zip(y_degrees.into_iter())
                .map(|((point, x_degrees), y_degrees)| {
                    let point: geo::Point = point.into();
                    AffineTransform::skew(x_degrees, y_degrees, point)
                })
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
        TransformOrigin::Center => {
            let centers = array.center();
            let transforms: Vec<AffineTransform> = centers
                .values_iter()
                .zip(x_degrees.into_iter())
                .zip(y_degrees.into_iter())
                .map(|((point, x_degrees), y_degrees)| {
                    let point: geo::Point = point.into();
                    AffineTransform::skew(x_degrees, y_degrees, point)
                })
                .collect();
            affine_transform(array, BroadcastableVec::Array(transforms))
        }
        TransformOrigin::Point(point) => {
            // Note: We need to unpack the enum here because otherwise the scalar will iter forever
            let transforms = match (x_degrees, y_degrees) {
                (
                    BroadcastablePrimitive::Scalar(x_degrees),
                    BroadcastablePrimitive::Scalar(y_degrees),
                ) => BroadcastableVec::Scalar(AffineTransform::skew(x_degrees, y_degrees, point)),
                (
                    BroadcastablePrimitive::Scalar(x_degrees),
                    BroadcastablePrimitive::Array(y_degrees),
                ) => {
                    let transforms: Vec<AffineTransform> = y_degrees
                        .values_iter()
                        .map(|y_degrees| AffineTransform::skew(x_degrees, *y_degrees, point))
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
                (
                    BroadcastablePrimitive::Array(x_degrees),
                    BroadcastablePrimitive::Scalar(y_degrees),
                ) => {
                    let transforms: Vec<AffineTransform> = x_degrees
                        .values_iter()
                        .map(|x_degrees| AffineTransform::skew(*x_degrees, y_degrees, point))
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
                (
                    BroadcastablePrimitive::Array(x_degrees),
                    BroadcastablePrimitive::Array(y_degrees),
                ) => {
                    let transforms: Vec<AffineTransform> = x_degrees
                        .values_iter()
                        .zip(y_degrees.values_iter())
                        .map(|(x_degrees, y_degrees)| {
                            AffineTransform::skew(*x_degrees, *y_degrees, point)
                        })
                        .collect();
                    BroadcastableVec::Array(transforms)
                }
            };

            affine_transform(array, transforms)
        }
    }
}
