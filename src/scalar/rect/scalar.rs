use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

use crate::algorithm::native::eq::rect_eq;
use crate::array::SeparatedCoordBuffer;
use crate::geo_traits::RectTrait;
use crate::io::geo::rect_to_geo;
use crate::trait_::GeometryScalarTrait;

#[derive(Debug, Clone)]
pub struct Rect<'a, const D: usize> {
    lower: Cow<'a, SeparatedCoordBuffer<D>>,
    upper: Cow<'a, SeparatedCoordBuffer<D>>,
    pub(crate) geom_index: usize,
}

impl<'a, const D: usize> Rect<'a, D> {
    pub fn new(
        lower: Cow<'a, SeparatedCoordBuffer<D>>,
        upper: Cow<'a, SeparatedCoordBuffer<D>>,
        geom_index: usize,
    ) -> Self {
        Self {
            lower,
            upper,
            geom_index,
        }
    }

    pub fn new_borrowed(
        lower: &'a SeparatedCoordBuffer<D>,
        upper: &'a SeparatedCoordBuffer<D>,
        geom_index: usize,
    ) -> Self {
        Self {
            lower: Cow::Borrowed(lower),
            upper: Cow::Borrowed(upper),
            geom_index,
        }
    }

    pub fn new_owned(
        lower: SeparatedCoordBuffer<D>,
        upper: SeparatedCoordBuffer<D>,
        geom_index: usize,
    ) -> Self {
        Self {
            lower: Cow::Owned(lower),
            upper: Cow::Owned(upper),
            geom_index,
        }
    }

    pub fn into_owned_inner(self) -> (SeparatedCoordBuffer<D>, SeparatedCoordBuffer<D>, usize) {
        // TODO: make hard slice?
        (
            self.lower.into_owned(),
            self.upper.into_owned(),
            self.geom_index,
        )
    }
}

impl<'a, const D: usize> GeometryScalarTrait for Rect<'a, D> {
    type ScalarGeo = geo::Rect;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::Rect(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        todo!()
        // self.try_into()
    }
}

// TODO: support 3d rects
impl<'a, const D: usize> RectTrait for Rect<'a, D> {
    type T = f64;
    type ItemType<'b> = (Self::T, Self::T) where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn lower(&self) -> Self::ItemType<'_> {
        let minx = self.lower.buffers[0][self.geom_index];
        let miny = self.lower.buffers[1][self.geom_index];
        (minx, miny)
    }

    fn upper(&self) -> Self::ItemType<'_> {
        let maxx = self.upper.buffers[0][self.geom_index];
        let maxy = self.upper.buffers[1][self.geom_index];
        (maxx, maxy)
    }
}

impl<const D: usize> From<Rect<'_, D>> for geo::Rect {
    fn from(value: Rect<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&Rect<'_, D>> for geo::Rect {
    fn from(value: &Rect<'_, D>) -> Self {
        rect_to_geo(value)
    }
}

impl<const D: usize> From<Rect<'_, D>> for geo::Geometry {
    fn from(value: Rect<'_, D>) -> Self {
        geo::Geometry::Rect(value.into())
    }
}

impl<const D: usize> RTreeObject for Rect<'_, D> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (minx, miny) = self.lower();
        let (maxx, maxy) = self.upper();
        AABB::from_corners([minx, miny], [maxx, maxy])
    }
}

impl<G: RectTrait<T = f64>, const D: usize> PartialEq<G> for Rect<'_, D> {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
