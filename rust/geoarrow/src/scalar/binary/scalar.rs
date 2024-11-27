use crate::error::Result;
use crate::trait_::NativeScalar;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};
use geo::BoundingRect;
use geo_traits::to_geo::ToGeoGeometry;
use geo_traits::GeometryTrait;
use rstar::{RTreeObject, AABB};

/// A scalar WKB reference on a WKBArray
///
/// This is zero-cost to _create_ from a [WKBArray] but the WKB has not been preprocessed yet, so
/// it's not constant-time to access coordinate values.
#[derive(Debug, Clone)]
pub struct WKB<'a, O: OffsetSizeTrait> {
    pub(crate) arr: &'a GenericBinaryArray<O>,
    pub(crate) geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> WKB<'a, O> {
    /// Construct a new WKB.
    pub(crate) fn new(arr: &'a GenericBinaryArray<O>, geom_index: usize) -> Self {
        Self { arr, geom_index }
    }

    /// Access the byte slice of this WKB object.
    pub fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    pub fn into_owned_inner(self) -> (GenericBinaryArray<O>, usize) {
        // TODO: hard slice?
        // let owned = self.into_owned();
        (self.arr.clone(), self.geom_index)
    }

    pub fn parse(&self) -> Result<impl GeometryTrait<T = f64> + use<'_, O>> {
        Ok(wkb::reader::read_wkb(self.as_ref())?)
    }
}

impl<'a, O: OffsetSizeTrait> NativeScalar for WKB<'a, O> {
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

// impl<O: OffsetSizeTrait> TryFrom<&WKB<'_, O>> for geo::Geometry {
//     type Error = GeoArrowError;
//     fn try_from(value: &WKB<'_, O>) -> std::result::Result<Self, Self::Error> {
//         Ok(geometry_to_geo(&value.parse()?))
//     }
// }

// impl<O: OffsetSizeTrait> TryFrom<WKB<'_, O>> for geo::Geometry {
//     type Error = GeoArrowError;
//     fn try_from(value: WKB<'_, O>) -> std::result::Result<Self, Self::Error> {
//         (&value).try_into()
//     }
// }

impl<O: OffsetSizeTrait> From<&WKB<'_, O>> for geo::Geometry {
    fn from(value: &WKB<'_, O>) -> Self {
        value.parse().unwrap().to_geometry()
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
