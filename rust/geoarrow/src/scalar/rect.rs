use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::rect_eq;
use crate::array::RectArray;
use crate::scalar::SeparatedCoord;
use crate::trait_::NativeScalar;
use crate::ArrayBase;
use geo_traits::to_geo::ToGeoRect;
use geo_traits::RectTrait;

/// An Arrow equivalent of a Rect
///
/// This is stored as a [RectArray] with length 1. That element may not be null.
#[derive(Debug, Clone)]
pub struct Rect(RectArray);

impl Rect {
    pub fn new(array: RectArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        Self(array)
    }
    pub fn into_inner(self) -> RectArray {
        self.0
    }
}

impl NativeScalar for Rect {
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
impl<'a> RectTrait for Rect {
    type T = f64;
    type CoordType<'b>
        = SeparatedCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.lower.dim.into()
    }

    fn min(&self) -> Self::CoordType<'_> {
        self.0.lower.value(0)
    }

    fn max(&self) -> Self::CoordType<'_> {
        self.0.upper.value(0)
    }
}

impl From<Rect> for geo::Rect {
    fn from(value: Rect) -> Self {
        (&value).into()
    }
}

impl From<&Rect> for geo::Rect {
    fn from(value: &Rect) -> Self {
        value.to_rect()
    }
}

impl From<Rect> for geo::Geometry {
    fn from(value: Rect) -> Self {
        geo::Geometry::Rect(value.into())
    }
}

impl RTreeObject for Rect {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let lower = self.min();
        let lower = core::array::from_fn(|i| lower.buffers[i][0]);

        let upper = self.max();
        let upper = core::array::from_fn(|i| upper.buffers[i][0]);

        AABB::from_corners(lower, upper)
    }
}

impl<G: RectTrait<T = f64>> PartialEq<G> for Rect {
    fn eq(&self, other: &G) -> bool {
        rect_eq(self, other)
    }
}
