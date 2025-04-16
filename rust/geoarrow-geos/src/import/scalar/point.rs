use geo_traits::PointTrait;
use geoarrow_array::error::{GeoArrowError, Result};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::coord::GEOSConstCoord;

pub struct GEOSPoint(geos::Geometry);

impl GEOSPoint {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Point) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be point".to_string(),
            ))
        }
    }
}

impl PointTrait for GEOSPoint {
    type T = f64;
    type CoordType<'a>
        = GEOSConstCoord
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let is_empty = self.0.is_empty().unwrap();
        if is_empty {
            None
        } else {
            Some(GEOSConstCoord {
                coords: self.0.get_coord_seq().unwrap(),
                geom_index: 0,
                dim: self.dim(),
            })
        }
    }
}

impl PointTrait for &GEOSPoint {
    type T = f64;
    type CoordType<'a>
        = GEOSConstCoord
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let is_empty = self.0.is_empty().unwrap();
        if is_empty {
            None
        } else {
            Some(GEOSConstCoord {
                coords: self.0.get_coord_seq().unwrap(),
                geom_index: 0,
                dim: self.dim(),
            })
        }
    }
}

pub struct GEOSConstPoint<'a>(geos::ConstGeometry<'a>);

impl<'a> GEOSConstPoint<'a> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a>) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::ConstGeometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Point) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be point".to_string(),
            ))
        }
    }
}

impl PointTrait for GEOSConstPoint<'_> {
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

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let is_empty = self.0.is_empty().unwrap();
        if is_empty {
            None
        } else {
            Some(GEOSConstCoord {
                coords: self.0.get_coord_seq().unwrap(),
                geom_index: 0,
                dim: self.dim(),
            })
        }
    }
}

impl PointTrait for &GEOSConstPoint<'_> {
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

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let is_empty = self.0.is_empty().unwrap();
        if is_empty {
            None
        } else {
            Some(GEOSConstCoord {
                coords: self.0.get_coord_seq().unwrap(),
                geom_index: 0,
                dim: self.dim(),
            })
        }
    }
}
