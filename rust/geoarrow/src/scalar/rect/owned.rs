use crate::algorithm::native::eq::rect_eq;
use crate::array::{RectArray, SeparatedCoordBuffer};
use crate::scalar::{Rect, SeparatedCoord};
use geo_traits::RectTrait;

#[derive(Clone, Debug)]
pub struct OwnedRect<const D: usize> {
    lower: SeparatedCoordBuffer,
    upper: SeparatedCoordBuffer,
    geom_index: usize,
}

impl<const D: usize> OwnedRect<D> {
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

impl<'a, const D: usize> From<&'a OwnedRect<D>> for Rect<'a, D> {
    fn from(value: &'a OwnedRect<D>) -> Self {
        Self::new(&value.lower, &value.upper, value.geom_index)
    }
}

impl<'a, const D: usize> From<Rect<'a, D>> for OwnedRect<D> {
    fn from(value: Rect<'a, D>) -> Self {
        let (lower, upper, geom_index) = value.into_owned_inner();
        Self::new(lower, upper, geom_index)
    }
}

impl<const D: usize> From<OwnedRect<D>> for RectArray<D> {
    fn from(value: OwnedRect<D>) -> Self {
        Self::new(value.lower, value.upper, None, Default::default())
    }
}

impl<const D: usize> RectTrait for OwnedRect<D> {
    type T = f64;
    type CoordType<'b> = SeparatedCoord<'b, D> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        // TODO: pass through field information from array
        match D {
            2 => geo_traits::Dimensions::Xy,
            3 => geo_traits::Dimensions::Xyz,
            _ => todo!(),
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
