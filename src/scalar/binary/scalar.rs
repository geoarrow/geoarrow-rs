use crate::trait_::GeometryScalarTrait;
use arrow2::array::BinaryArray;
use arrow2::types::Offset;
use geo::BoundingRect;
#[cfg(feature = "geozero")]
use geozero::ToGeo;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Point
#[derive(Debug, Clone)]
pub struct WKB<'a, O: Offset> {
    pub arr: &'a BinaryArray<O>,
    pub geom_index: usize,
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for WKB<'a, O> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

#[cfg(feature = "geozero")]
impl<O: Offset> From<WKB<'_, O>> for geo::Geometry {
    fn from(value: WKB<'_, O>) -> Self {
        (&value).into()
    }
}

#[cfg(feature = "geozero")]
impl<O: Offset> From<&WKB<'_, O>> for geo::Geometry {
    fn from(value: &WKB<'_, O>) -> Self {
        let buf = value.arr.value(value.geom_index);
        geozero::wkb::Wkb(buf.to_vec()).to_geo().unwrap()
    }
}

#[cfg(not(feature = "geozero"))]
impl<O: Offset> From<WKB<'_, O>> for geo::Geometry {
    fn from(_value: WKB<'_, O>) -> Self {
        (&_value).into()
    }
}

#[cfg(not(feature = "geozero"))]
impl<O: Offset> From<&WKB<'_, O>> for geo::Geometry {
    fn from(_value: &WKB<'_, O>) -> Self {
        panic!("Activate the 'geozero' feature to convert WKB items to geo::Geometry.")
    }
}

impl<O: Offset> RTreeObject for WKB<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let geom: geo::Geometry = self.into();
        let rect = geom.bounding_rect().unwrap();
        let lower: [f64; 2] = rect.min().into();
        let upper: [f64; 2] = rect.max().into();
        AABB::from_corners(lower, upper)
    }
}

impl<O: Offset> PartialEq for WKB<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        self.arr.value(self.geom_index) == other.arr.value(other.geom_index)
    }
}
