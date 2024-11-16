use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::rect_eq;
use crate::array::SeparatedCoordBuffer;
use crate::io::geo::rect_to_geo;
use crate::scalar::SeparatedCoord;
use crate::trait_::NativeScalar;
use geo_traits::RectTrait;

#[derive(Debug, Clone)]
pub struct Rect<'a> {
    lower: &'a SeparatedCoordBuffer,
    upper: &'a SeparatedCoordBuffer,
    pub(crate) geom_index: usize,
}

impl<'a> Rect<'a> {
    pub fn new(
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
    pub fn into_owned_inner(self) -> (SeparatedCoordBuffer, SeparatedCoordBuffer, usize) {
        // TODO: make hard slice?
        (self.lower.clone(), self.upper.clone(), self.geom_index)
    }
}

impl<'a> NativeScalar for Rect<'a> {
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
impl<'a> RectTrait for Rect<'a> {
    type T = f64;
    type CoordType<'b> = SeparatedCoord<'a> where Self: 'b;

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

impl From<Rect<'_>> for geo::Rect {
    fn from(value: Rect<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Rect<'_>> for geo::Rect {
    fn from(value: &Rect<'_>) -> Self {
        rect_to_geo(value)
    }
}

impl From<Rect<'_>> for geo::Geometry {
    fn from(value: Rect<'_>) -> Self {
        geo::Geometry::Rect(value.into())
    }
}

impl RTreeObject for Rect<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let lower = self.min();
        let lower = core::array::from_fn(|i| lower.buffers[i][self.geom_index]);

        let upper = self.max();
        let upper = core::array::from_fn(|i| upper.buffers[i][self.geom_index]);

        AABB::from_corners(lower, upper)
    }
}

impl<G: RectTrait<T = f64>> PartialEq<G> for Rect<'_> {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
