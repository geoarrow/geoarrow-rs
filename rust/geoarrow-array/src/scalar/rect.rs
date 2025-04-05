use geo_traits::RectTrait;

use crate::array::SeparatedCoordBuffer;
use crate::eq::rect_eq;
use crate::scalar::SeparatedCoord;

/// An Arrow equivalent of a Rect
///
/// This implements [RectTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct Rect<'a> {
    lower: &'a SeparatedCoordBuffer,
    upper: &'a SeparatedCoordBuffer,
    pub(crate) geom_index: usize,
}

impl<'a> Rect<'a> {
    pub(crate) fn new(
        lower: &'a SeparatedCoordBuffer,
        upper: &'a SeparatedCoordBuffer,
        geom_index: usize,
    ) -> Self {
        Self {
            lower,
            upper,
            geom_index,
        }
    }
}

// TODO: support 3d rects
impl<'a> RectTrait for Rect<'a> {
    type T = f64;
    type CoordType<'b>
        = SeparatedCoord<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.lower.dim.into()
    }

    fn min(&self) -> Self::CoordType<'_> {
        self.lower.value(self.geom_index)
    }

    fn max(&self) -> Self::CoordType<'_> {
        self.upper.value(self.geom_index)
    }
}

impl<G: RectTrait<T = f64>> PartialEq<G> for Rect<'_> {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
