use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiLineStringTrait;
use crate::io::geos::scalar::GEOSConstLineString;
use crate::scalar::MultiLineString;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<'a, O: OffsetSizeTrait, const D: usize> TryFrom<&'a MultiLineString<'_, O, D>>
    for geos::Geometry
{
    type Error = geos::Error;

    fn try_from(
        value: &'a MultiLineString<'_, O, D>,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_multiline_string(
            value
                .lines()
                .map(|line| (&line).try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}
/// A GEOS geometry known to be a MultiLineString
#[derive(Clone)]
pub struct GEOSMultiLineString(pub(crate) geos::Geometry);

impl GEOSMultiLineString {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
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
    pub fn line(&self, i: usize) -> Option<GEOSConstLineString<'_>> {
        if i > (self.num_lines()) {
            return None;
        }

        Some(GEOSConstLineString::new_unchecked(
            self.0.get_geometry_n(i).unwrap(),
        ))
    }
}

impl MultiLineStringTrait for GEOSMultiLineString {
    type T = f64;
    type ItemType<'a> = GEOSConstLineString<'a> where Self: 'a;

    fn dim(&self) -> usize {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => 2,
            geos::Dimensions::ThreeD => 3,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_lines(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        GEOSConstLineString::new_unchecked(self.0.get_geometry_n(i).unwrap())
    }
}
