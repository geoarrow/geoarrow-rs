use crate::algorithm::broadcasting::{BroadcastablePrimitive, BroadcastableVec};
use crate::algorithm::geo::affine::affine_transform;
use crate::array::GeometryArray;
use crate::error::Result;
use geo::AffineTransform;

pub fn translate(
    array: &GeometryArray,
    x_offset: BroadcastablePrimitive<f64>,
    y_offset: BroadcastablePrimitive<f64>,
) -> Result<GeometryArray> {
    // TODO: validate lengths between broadcasting elements

    // Note: We need to unpack the enum here because otherwise the scalar will iter forever
    let transforms = match (x_offset, y_offset) {
        (BroadcastablePrimitive::Scalar(x_offset), BroadcastablePrimitive::Scalar(y_offset)) => {
            BroadcastableVec::Scalar(AffineTransform::translate(x_offset, y_offset))
        }
        (BroadcastablePrimitive::Scalar(x_offset), BroadcastablePrimitive::Array(y_offset)) => {
            let transforms: Vec<AffineTransform> = y_offset
                .values_iter()
                .map(|y_offset| AffineTransform::translate(x_offset, *y_offset))
                .collect();
            BroadcastableVec::Array(transforms)
        }
        (BroadcastablePrimitive::Array(x_offset), BroadcastablePrimitive::Scalar(y_offset)) => {
            let transforms: Vec<AffineTransform> = x_offset
                .values_iter()
                .map(|x_offset| AffineTransform::translate(*x_offset, y_offset))
                .collect();
            BroadcastableVec::Array(transforms)
        }
        (BroadcastablePrimitive::Array(x_offset), BroadcastablePrimitive::Array(y_offset)) => {
            let transforms: Vec<AffineTransform> = x_offset
                .values_iter()
                .zip(y_offset.values_iter())
                .map(|(x_offset, y_offset)| AffineTransform::translate(*x_offset, *y_offset))
                .collect();
            BroadcastableVec::Array(transforms)
        }
    };

    affine_transform(array, transforms)
}
