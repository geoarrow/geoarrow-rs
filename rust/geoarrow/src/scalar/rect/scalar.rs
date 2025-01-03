use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::rect_eq;
use crate::array::SeparatedCoordBuffer;
use crate::scalar::SeparatedCoord;
use crate::trait_::NativeScalar;
use geo_traits::to_geo::ToGeoRect;
use geo_traits::RectTrait;

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

    pub(crate) fn into_owned_inner(self) -> (SeparatedCoordBuffer, SeparatedCoordBuffer, usize) {
        // TODO: make hard slice?
        (self.lower.clone(), self.upper.clone(), self.geom_index)
    }
}

impl NativeScalar for Rect<'_> {
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

impl<'a> RectTrait for Rect<'a> {
    type T = f64;
    type CoordType<'b>
        = SeparatedCoord<'a>
    where
        Self: 'b;

    #[inline]
    fn dim(&self) -> geo_traits::Dimensions {
        self.lower.dim.into()
    }

    #[inline]
    fn min(&self) -> Self::CoordType<'_> {
        self.lower.value(self.geom_index)
    }

    #[inline]
    fn max(&self) -> Self::CoordType<'_> {
        self.upper.value(self.geom_index)
    }
}

impl<'a> RectTrait for &'a Rect<'a> {
    type T = f64;
    type CoordType<'b>
        = SeparatedCoord<'a>
    where
        Self: 'b;

    #[inline]
    fn dim(&self) -> geo_traits::Dimensions {
        self.lower.dim.into()
    }

    #[inline]
    fn min(&self) -> Self::CoordType<'_> {
        self.lower.value(self.geom_index)
    }

    #[inline]
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
        value.to_rect()
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
