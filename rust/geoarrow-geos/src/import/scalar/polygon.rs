use geo_traits::PolygonTrait;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::linearring::GEOSConstLinearRing;

pub struct GEOSPolygon(pub(crate) geos::Geometry);

impl GEOSPolygon {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> GeoArrowResult<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Polygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::IncorrectGeometryType(
                "Geometry type must be polygon".to_string(),
            ))
        }
    }

    pub(crate) fn dimension(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }
}

impl PolygonTrait for GEOSPolygon {
    type RingType<'a>
        = GEOSConstLinearRing<'a>
    where
        Self: 'a;

    fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        GEOSConstLinearRing::new_unchecked(
            self.0.get_interior_ring_n(i.try_into().unwrap()).unwrap(),
        )
    }
}

pub struct GEOSConstPolygon<'a>(pub(crate) geos::ConstGeometry<'a>);

impl<'a> GEOSConstPolygon<'a> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a>) -> GeoArrowResult<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Polygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::IncorrectGeometryType(
                "Geometry type must be polygon".to_string(),
            ))
        }
    }

    pub(crate) fn dimension(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }
}

impl PolygonTrait for GEOSConstPolygon<'_> {
    type RingType<'c>
        = GEOSConstLinearRing<'c>
    where
        Self: 'c;

    fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        GEOSConstLinearRing::new_unchecked(
            self.0.get_interior_ring_n(i.try_into().unwrap()).unwrap(),
        )
    }
}
