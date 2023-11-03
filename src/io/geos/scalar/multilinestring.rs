use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiLineStringTrait;
use crate::io::geos::scalar::GEOSConstLineString;
use crate::scalar::MultiLineString;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<'b, O: OffsetSizeTrait> TryFrom<MultiLineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiLineString<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a MultiLineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a MultiLineString<'_, O>) -> Result<geos::Geometry<'b>> {
        let num_lines = value.num_lines();
        let mut geos_geoms = Vec::with_capacity(num_lines);

        for line_idx in 0..num_lines {
            let line = value.line(line_idx).unwrap();
            geos_geoms.push(line.try_into()?);
        }

        Ok(geos::Geometry::create_multiline_string(geos_geoms)?)
    }
}

#[derive(Clone)]
pub struct GEOSMultiLineString<'a>(pub(crate) geos::Geometry<'a>);

impl<'a> GEOSMultiLineString<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::MultiLineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be multi line string".to_string(),
            ))
        }
    }

    pub fn num_lines(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    #[allow(dead_code)]
    pub fn line(&'a self, i: usize) -> Option<GEOSConstLineString<'a, '_>> {
        if i > (self.num_lines()) {
            return None;
        }

        Some(GEOSConstLineString::new_unchecked(
            self.0.get_geometry_n(i).unwrap(),
        ))
    }
}
