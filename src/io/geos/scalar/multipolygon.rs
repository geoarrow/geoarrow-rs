use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPolygonTrait;
use crate::io::geos::scalar::GEOSConstPolygon;
use crate::scalar::MultiPolygon;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<O: OffsetSizeTrait> TryFrom<MultiPolygon<'_, O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: MultiPolygon<'_, O>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<&'a MultiPolygon<'_, O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(
        value: &'a MultiPolygon<'_, O>,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_multipolygon(
            value
                .polygons()
                .map(|polygons| polygons.try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}

#[derive(Clone)]
pub struct GEOSMultiPolygon(pub(crate) geos::Geometry);

impl GEOSMultiPolygon {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::MultiPolygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be multi polygon".to_string(),
            ))
        }
    }
}

impl MultiPolygonTrait for GEOSMultiPolygon {
    type T = f64;
    type ItemType<'a> = GEOSConstPolygon<'a> where Self: 'a;

    fn num_polygons(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GEOSConstPolygon::new_unchecked(self.0.get_geometry_n(i).unwrap())
    }
}
