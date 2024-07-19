use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PolygonTrait;
use crate::io::geos::scalar::GEOSConstLinearRing;
use crate::scalar::Polygon;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<O: OffsetSizeTrait, const D: usize> TryFrom<Polygon<'_, O, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: Polygon<'_, O, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> TryFrom<&'a Polygon<'_, O, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a Polygon<'_, O, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        if let Some(exterior) = value.exterior() {
            let exterior = exterior.to_geos_linear_ring()?;
            let interiors = value
                .interiors()
                .map(|interior| interior.to_geos_linear_ring())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?;
            geos::Geometry::create_polygon(exterior, interiors)
        } else {
            geos::Geometry::create_empty_polygon()
        }
    }
}

#[derive(Clone)]
pub struct GEOSPolygon(pub(crate) geos::Geometry);

impl GEOSPolygon {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Polygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be polygon".to_string(),
            ))
        }
    }

    // TODO: delete these
    #[allow(dead_code)]
    pub fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    #[allow(dead_code)]
    pub fn exterior(&self) -> Option<GEOSConstLinearRing<'_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    #[allow(dead_code)]
    pub fn interior(&self, i: usize) -> Option<GEOSConstLinearRing<'_>> {
        if i > self.num_interiors() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_interior_ring_n(i.try_into().unwrap()).unwrap(),
        ))
    }
}

impl PolygonTrait for GEOSPolygon {
    type T = f64;
    type ItemType<'a> = GEOSConstLinearRing<'a> where Self: 'a;

    fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
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
    pub fn try_new(geom: geos::ConstGeometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Polygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be polygon".to_string(),
            ))
        }
    }
}

impl<'a> PolygonTrait for GEOSConstPolygon<'a> {
    type T = f64;
    type ItemType<'c> = GEOSConstLinearRing< 'c> where Self: 'c;

    fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GEOSConstLinearRing::new_unchecked(
            self.0.get_interior_ring_n(i.try_into().unwrap()).unwrap(),
        )
    }
}
