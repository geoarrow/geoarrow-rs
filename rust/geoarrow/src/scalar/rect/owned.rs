use crate::algorithm::native::eq::rect_eq;
use crate::array::{RectArray, SeparatedCoordBuffer};
use crate::scalar::{Rect, SeparatedCoord};
use geo_traits::RectTrait;

#[derive(Clone, Debug)]
pub struct OwnedRect {
    lower: SeparatedCoordBuffer,
    upper: SeparatedCoordBuffer,
    geom_index: usize,
}

impl OwnedRect<D> {
    pub fn new(
        lower: SeparatedCoordBuffer,
        upper: SeparatedCoordBuffer,
        geom_index: usize,
    ) -> Self {
        Self {
            lower,
            upper,
            geom_index,
        }
    }
}

impl<'a> From<&'a OwnedRect<D>> for Rect<'a> {
    fn from(value: &'a OwnedRect<D>) -> Self {
        Self::new(&value.lower, &value.upper, value.geom_index)
    }
}

impl<'a> From<Rect<'a>> for OwnedRect<D> {
    fn from(value: Rect<'a>) -> Self {
        let (lower, upper, geom_index) = value.into_owned_inner();
        Self::new(lower, upper, geom_index)
    }
}

impl From<OwnedRect<D>> for RectArray<D> {
    fn from(value: OwnedRect<D>) -> Self {
        Self::new(value.lower, value.upper, None, Default::default())
    }
}

impl RectTrait for OwnedRect<D> {
    type T = f64;
    type CoordType<'b> = SeparatedCoord<'b> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn min(&self) -> Self::CoordType<'_> {
        self.lower.value(self.geom_index)
    }

    fn max(&self) -> Self::CoordType<'_> {
        self.upper.value(self.geom_index)
    }
}

impl<G: RectTrait<T = f64>> PartialEq<G> for OwnedRect<2> {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
