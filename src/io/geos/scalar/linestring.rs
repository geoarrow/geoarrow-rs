use crate::array::util::OffsetBufferUtils;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::GEOSPoint;
use crate::scalar::LineString;
use geos::{Geom, GeometryTypes};

impl<'a, const D: usize> TryFrom<&'a LineString<'_, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a LineString<'_, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        let (start, end) = value.geom_offsets.start_end(value.geom_index);

        let sliced_coords = value.coords.clone().slice(start, end - start);

        geos::Geometry::create_line_string(sliced_coords.try_into()?)
    }
}

impl<const D: usize> LineString<'_, D> {
    pub fn to_geos_linear_ring(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);

        let sliced_coords = self.coords.clone().slice(start, end - start);

        geos::Geometry::create_linear_ring(sliced_coords.try_into()?)
    }
}

#[derive(Clone)]
pub struct GEOSLineString(geos::Geometry);

impl GEOSLineString {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

impl LineStringTrait for GEOSLineString {
    type T = f64;
    type ItemType<'b> = GEOSPoint where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimension {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimension::XY,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimension::XYZ,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}

impl LineStringTrait for &GEOSLineString {
    type T = f64;
    type ItemType<'b> = GEOSPoint where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimension {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimension::XY,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimension::XYZ,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}

pub struct GEOSConstLineString<'a>(geos::ConstGeometry<'a>);

impl<'a> GEOSConstLineString<'a> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

impl<'a> LineStringTrait for GEOSConstLineString<'a> {
    type T = f64;
    type ItemType<'c> = GEOSPoint where Self: 'c;

    fn dim(&self) -> crate::geo_traits::Dimension {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimension::XY,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimension::XYZ,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}

impl<'a> LineStringTrait for &'a GEOSConstLineString<'a> {
    type T = f64;
    type ItemType<'c> = GEOSPoint where Self: 'c;

    fn dim(&self) -> crate::geo_traits::Dimension {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimension::XY,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimension::XYZ,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSPoint::new_unchecked(point)
    }
}
