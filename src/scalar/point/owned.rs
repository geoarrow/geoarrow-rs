use crate::array::PointArray;
use crate::scalar::Point;
use crate::GeometryArrayTrait;

pub struct OwnedPoint(PointArray);

impl<'a> From<&'a OwnedPoint> for Point<'a> {
    fn from(value: &'a OwnedPoint) -> Self {
        value.0.value(0)
    }
}
