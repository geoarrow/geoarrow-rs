use crate::io::geo::geometry_to_geo;
use crate::trait_::GeometryScalarTrait;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use geo::BoundingRect;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Point
#[derive(Debug, Clone)]
pub struct WKB<'a, O: OffsetSizeTrait> {
    pub(crate) arr: &'a GenericBinaryArray<O>,
    pub(crate) geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> WKB<'a, O> {
    pub fn new(arr: &'a GenericBinaryArray<O>, geom_index: usize) -> Self {
        Self { arr, geom_index }
    }

    pub fn into_owned_inner(self) -> (GenericBinaryArray<O>, usize) {
        // TODO: hard slice?
        // let owned = self.into_owned();
        (self.arr.clone(), self.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for WKB<'a, O> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        self.to_geo()
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, O: OffsetSizeTrait> AsRef<[u8]> for WKB<'a, O> {
    fn as_ref(&self) -> &[u8] {
        self.arr.value(self.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<&WKB<'_, O>> for geo::Geometry {
    fn from(value: &WKB<'_, O>) -> Self {
        geometry_to_geo(&value.to_wkb_object())
    }
}

impl<O: OffsetSizeTrait> From<WKB<'_, O>> for geo::Geometry {
    fn from(value: WKB<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> RTreeObject for WKB<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let geom: geo::Geometry = self.into();
        let rect = geom.bounding_rect().unwrap();
        let lower: [f64; 2] = rect.min().into();
        let upper: [f64; 2] = rect.max().into();
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait> PartialEq for WKB<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        self.arr.value(self.geom_index) == other.arr.value(other.geom_index)
    }
}
