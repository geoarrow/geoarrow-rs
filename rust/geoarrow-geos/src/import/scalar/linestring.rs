use geo_traits::LineStringTrait;
use geoarrow_array::error::{GeoArrowError, Result};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::coord::GEOSConstCoord;

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
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}

impl LineStringTrait for &GEOSLineString {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
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

impl LineStringTrait for GEOSConstLineString<'_> {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}

impl<'a> LineStringTrait for &'a GEOSConstLineString<'a> {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}
