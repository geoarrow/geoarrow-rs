use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::coord::GEOSConstCoord;
use crate::scalar::Point;
use geo_traits::PointTrait;
use geos::{Geom, GeometryTypes};

impl<'a> TryFrom<&'a Point<'_>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(point: &'a Point<'_>) -> std::result::Result<geos::Geometry, geos::Error> {
        if let Some(coord) = PointTrait::coord(&point) {
            let coord_seq = (&coord).try_into()?;
            Ok(geos::Geometry::create_point(coord_seq)?)
        } else {
            Ok(geos::Geometry::create_empty_point()?)
        }
    }
}

#[derive(Clone)]
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
    type CoordType<'a> = GEOSConstCoord where Self: 'a;

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
    type CoordType<'a> = GEOSConstCoord where Self: 'a;

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

impl<'a> PointTrait for GEOSConstPoint<'a> {
    type T = f64;
    type CoordType<'b> = GEOSConstCoord where Self: 'b;

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

impl<'a> PointTrait for &GEOSConstPoint<'a> {
    type T = f64;
    type CoordType<'b> = GEOSConstCoord where Self: 'b;

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

impl Clone for GEOSConstPoint<'_> {
    fn clone(&self) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::algorithm::native::eq::point_eq;
    use crate::test::point;
    use crate::trait_::ArrayAccessor;

    #[test]
    fn round_trip_point() {
        let arr = point::point_array();
        let scalar = arr.value(0);
        let geom = geos::Geometry::try_from(&scalar).unwrap();
        let geos_pt = GEOSPoint::new_unchecked(geom);
        assert!(point_eq(&scalar, &geos_pt))
    }

    #[test]
    fn round_trip_point_z() {
        let arr = point::point_z_array();
        let scalar = arr.value(0);
        let geom = geos::Geometry::try_from(&scalar).unwrap();
        let geos_pt = GEOSPoint::new_unchecked(geom);
        assert!(point_eq(&scalar, &geos_pt))
    }
}
