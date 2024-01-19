use crate::array::util::OffsetBufferUtils;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::GEOSPoint;
use crate::scalar::LineString;
use crate::trait_::GeometryArraySelfMethods;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<'b, O: OffsetSizeTrait> TryFrom<LineString<'_, O>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(value: LineString<'_, O>) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

// TODO: maybe this should use traits instead of a manual approach via coordbuffer?
impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a LineString<'_, O>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(
        value: &'a LineString<'_, O>,
    ) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        let (start, end) = value.geom_offsets.start_end(value.geom_index);

        let sliced_coords = value.coords.clone().to_mut().slice(start, end - start);

        geos::Geometry::create_line_string(sliced_coords.try_into()?)
    }
}

impl<'b, O: OffsetSizeTrait> LineString<'_, O> {
    pub fn to_geos_linear_ring(&self) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);

        let sliced_coords = self.coords.clone().to_mut().slice(start, end - start);

        geos::Geometry::create_linear_ring(sliced_coords.try_into()?)
    }
}

#[derive(Clone)]
pub struct GEOSLineString<'a>(geos::Geometry<'a>);

impl<'a> GEOSLineString<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }
    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

impl<'a> LineStringTrait for GEOSLineString<'a> {
    type T = f64;
    type ItemType<'b> = GEOSPoint<'a> where Self: 'b;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}

impl<'a> LineStringTrait for &'a GEOSLineString<'a> {
    type T = f64;
    type ItemType<'b> = GEOSPoint<'a> where Self: 'b;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}

pub struct GEOSConstLineString<'a, 'b>(geos::ConstGeometry<'a, 'b>);

impl<'a, 'b> GEOSConstLineString<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

impl<'a, 'b> LineStringTrait for GEOSConstLineString<'a, 'b> {
    type T = f64;
    type ItemType<'c> = GEOSPoint<'a> where Self: 'c;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}

impl<'a, 'b> LineStringTrait for &'a GEOSConstLineString<'a, 'b> {
    type T = f64;
    type ItemType<'c> = GEOSPoint<'a> where Self: 'c;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}
