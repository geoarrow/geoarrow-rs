use crate::algorithm::native::eq::rect_eq;
use crate::geo_traits::RectTrait;
use crate::scalar::Rect;
use arrow_buffer::ScalarBuffer;

#[derive(Debug)]
pub struct OwnedRect {
    values: ScalarBuffer<f64>,

    geom_index: usize,
}

impl OwnedRect {
    pub fn new(values: ScalarBuffer<f64>, geom_index: usize) -> Self {
        Self { values, geom_index }
    }
}

impl<'a> From<OwnedRect> for Rect<'a> {
    fn from(value: OwnedRect) -> Self {
        Self::new_owned(value.values, value.geom_index)
    }
}

impl<'a> From<&'a OwnedRect> for Rect<'a> {
    fn from(value: &'a OwnedRect) -> Self {
        Self::new_borrowed(&value.values, value.geom_index)
    }
}

impl<'a> From<Rect<'a>> for OwnedRect {
    fn from(value: Rect<'a>) -> Self {
        let (values, geom_index) = value.into_owned_inner();
        Self::new(values, geom_index)
    }
}

impl RectTrait for OwnedRect {
    type T = f64;
    type ItemType<'b> = (Self::T, Self::T) where Self: 'b;

    fn lower(&self) -> Self::ItemType<'_> {
        Rect::from(self).lower()
    }

    fn upper(&self) -> Self::ItemType<'_> {
        Rect::from(self).upper()
    }
}

impl<G: RectTrait<T = f64>> PartialEq<G> for OwnedRect {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
