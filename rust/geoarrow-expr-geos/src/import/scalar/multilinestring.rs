use geo_traits::MultiLineStringTrait;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::linestring::GEOSConstLineString;

/// A GEOS geometry known to be a MultiLineString
pub struct GEOSMultiLineString(pub(crate) geos::Geometry);

impl GEOSMultiLineString {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> GeoArrowResult<Self> {
        if matches!(geom.geometry_type(), Ok(GeometryTypes::MultiLineString)) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::IncorrectGeometryType(
                "Geometry type must be multi line string".to_string(),
            ))
        }
    }

    pub fn num_lines(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    #[allow(dead_code)]
    pub fn line(&self, i: usize) -> Option<GEOSConstLineString<'_>> {
        if i > (self.num_lines()) {
            return None;
        }

        Some(GEOSConstLineString::new_unchecked(
            self.0.get_geometry_n(i).unwrap(),
        ))
    }

    pub(crate) fn dimension(&self) -> geo_traits::Dimensions {
        crate::import::scalar::dimensions_from_geom(&self.0)
    }
}

impl MultiLineStringTrait for GEOSMultiLineString {
    type InnerLineStringType<'a>
        = GEOSConstLineString<'a>
    where
        Self: 'a;

    fn num_line_strings(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::InnerLineStringType<'_> {
        GEOSConstLineString::new_unchecked(self.0.get_geometry_n(i).unwrap())
    }
}
