use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPolygonTrait;
use crate::io::geos::scalar::GEOSConstPolygon;
use crate::scalar::MultiPolygon;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};
use std::iter::Cloned;
use std::slice::Iter;

impl<'b, O: OffsetSizeTrait> TryFrom<MultiPolygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPolygon<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a MultiPolygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a MultiPolygon<'_, O>) -> Result<geos::Geometry<'b>> {
        let num_polygons = value.num_polygons();
        let mut geos_geoms = Vec::with_capacity(num_polygons);

        for polygon_idx in 0..num_polygons {
            let polygon = value.polygon(polygon_idx).unwrap();
            geos_geoms.push(polygon.try_into()?);
        }

        Ok(geos::Geometry::create_multipolygon(geos_geoms)?)
    }
}

#[derive(Clone)]
pub struct GEOSMultiPolygon<'a>(pub(crate) geos::Geometry<'a>);

impl<'a> GEOSMultiPolygon<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::MultiPolygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be multi polygon".to_string(),
            ))
        }
    }
}

impl<'a> MultiPolygonTrait for GEOSMultiPolygon<'a> {
    type T = f64;
    type ItemType<'c> = GEOSConstPolygon<'a, 'c> where Self: 'c;
    type Iter<'c> = Cloned<Iter<'c, Self::ItemType<'c>>> where Self: 'c;

    fn num_polygons(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > self.num_polygons() {
            return None;
        }

        Some(GEOSConstPolygon::new_unchecked(
            self.0.get_geometry_n(i).unwrap(),
        ))
    }

    fn polygons(&self) -> Self::Iter<'_> {
        todo!()
    }
}
