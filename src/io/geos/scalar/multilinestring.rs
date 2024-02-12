use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiLineStringTrait;
use crate::io::geos::scalar::GEOSConstLineString;
use crate::scalar::MultiLineString;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<'b, O: OffsetSizeTrait> TryFrom<MultiLineString<'_, O>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(
        value: MultiLineString<'_, O>,
    ) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a MultiLineString<'_, O>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(
        value: &'a MultiLineString<'_, O>,
    ) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        geos::Geometry::create_multiline_string(
            value
                .lines()
                .map(|line| line.try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}
/// A GEOS geometry known to be a MultiLineString
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

impl<'a> MultiLineStringTrait for GEOSMultiLineString<'a> {
    type T = f64;
    type ItemType<'c> = GEOSConstLineString<'a, 'c> where Self: 'c;

    fn num_lines(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GEOSConstLineString::new_unchecked(self.0.get_geometry_n(i).unwrap())
    }
}
