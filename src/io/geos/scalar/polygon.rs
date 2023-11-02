use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PolygonTrait;
use crate::io::geos::scalar::GEOSConstLinearRing;
use crate::scalar::Polygon;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<'b, O: OffsetSizeTrait> TryFrom<Polygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: Polygon<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a Polygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a Polygon<'_, O>) -> Result<geos::Geometry<'b>> {
        if let Some(exterior) = value.exterior() {
            let exterior = exterior.to_geos_linear_ring()?;
            let num_interiors = value.num_interiors();

            let mut interiors = Vec::with_capacity(num_interiors);

            for interior_idx in 0..num_interiors {
                let interior = value.interior(interior_idx).unwrap();
                interiors.push(interior.to_geos_linear_ring()?);
            }

            Ok(geos::Geometry::create_polygon(exterior, interiors)?)
        } else {
            Ok(geos::Geometry::create_empty_polygon()?)
        }
    }
}

#[derive(Clone)]
pub struct GEOSPolygon<'a>(pub(crate) geos::Geometry<'a>);

impl<'a> GEOSPolygon<'a> {
    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Polygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General("Geometry type must be polygon".to_string()))
        }
    }

    #[allow(dead_code)]
    pub fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    #[allow(dead_code)]
    pub fn exterior(&self) -> Option<GEOSConstLinearRing<'a, '_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    #[allow(dead_code)]
    pub fn interior(&self, i: usize) -> Option<GEOSConstLinearRing<'a, '_>> {
        if i > self.num_interiors() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_interior_ring_n(i.try_into().unwrap()).unwrap(),
        ))
    }
}

pub struct GEOSConstPolygon<'a, 'b>(pub(crate) geos::ConstGeometry<'a, 'b>);

impl<'a, 'b> GEOSConstPolygon<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Polygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General("Geometry type must be polygon".to_string()))
        }
    }

    pub fn num_interiors(&self) -> usize {
        self.0.get_num_interior_rings().unwrap()
    }

    pub fn exterior(&self) -> Option<GEOSConstLinearRing<'a, '_>> {
        if self.0.is_empty().unwrap() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_exterior_ring().unwrap(),
        ))
    }

    pub fn interior(&self, i: usize) -> Option<GEOSConstLinearRing<'a, '_>> {
        if i > self.num_interiors() {
            return None;
        }

        Some(GEOSConstLinearRing::new_unchecked(
            self.0.get_interior_ring_n(i.try_into().unwrap()).unwrap(),
        ))
    }
}
