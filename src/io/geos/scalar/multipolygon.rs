use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPolygonTrait;
use crate::io::geos::scalar::GEOSConstPolygon;
use crate::scalar::MultiPolygon;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<'b, O: OffsetSizeTrait> TryFrom<MultiPolygon<'_, O>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(
        value: MultiPolygon<'_, O>,
    ) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a MultiPolygon<'_, O>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(
        value: &'a MultiPolygon<'_, O>,
    ) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        geos::Geometry::create_multipolygon(
            value
                .polygons()
                .map(|polygons| polygons.try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
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

    fn num_polygons(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GEOSConstPolygon::new_unchecked(self.0.get_geometry_n(i).unwrap())
    }
}
