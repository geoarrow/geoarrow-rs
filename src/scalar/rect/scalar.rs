use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::rect_eq;
use crate::array::SeparatedCoordBuffer;
use crate::geo_traits::RectTrait;
use crate::io::geo::rect_to_geo;
use crate::trait_::NativeScalar;

#[derive(Debug, Clone)]
pub struct Rect<'a, const D: usize> {
    lower: &'a SeparatedCoordBuffer<D>,
    upper: &'a SeparatedCoordBuffer<D>,
    pub(crate) geom_index: usize,
}

impl<'a, const D: usize> Rect<'a, D> {
    pub fn new(
        lower: &'a SeparatedCoordBuffer<D>,
        upper: &'a SeparatedCoordBuffer<D>,
        geom_index: usize,
    ) -> Self {
        Self {
            lower,
            upper,
            geom_index,
        }
    }
    pub fn into_owned_inner(self) -> (SeparatedCoordBuffer<D>, SeparatedCoordBuffer<D>, usize) {
        // TODO: make hard slice?
        (self.lower.clone(), self.upper.clone(), self.geom_index)
    }
}

impl<'a, const D: usize> NativeScalar for Rect<'a, D> {
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
    type ItemType<'b> = [Self::T; D] where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn lower(&self) -> Self::ItemType<'_> {
        core::array::from_fn(|i| self.lower.buffers[i][self.geom_index])
    }

    fn upper(&self) -> Self::ItemType<'_> {
        core::array::from_fn(|i| self.upper.buffers[i][self.geom_index])
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
    type Envelope = AABB<[f64; D]>;

    fn envelope(&self) -> Self::Envelope {
        let lower = self.lower();
        let upper = self.upper();
        AABB::from_corners(lower, upper)
    }
}

impl<G: RectTrait<T = f64>, const D: usize> PartialEq<G> for Rect<'_, D> {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
