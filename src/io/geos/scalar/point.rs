use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PointTrait;
use crate::scalar::Point;
use geos::{CoordDimensions, CoordSeq, Geom, GeometryTypes};

impl<'a, const D: usize> TryFrom<&'a Point<'_, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(point: &'a Point<'_, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        use crate::geo_traits::Dimensions;

        match point.dim() {
            Dimension::XY | Dimension::Unknown(2) => {
                let mut coord_seq = CoordSeq::new(1, CoordDimensions::TwoD)?;
                coord_seq.set_x(0, point.x())?;
                coord_seq.set_y(0, point.y())?;

                Ok(geos::Geometry::create_point(coord_seq)?)
            }
            Dimension::XYZ | Dimension::Unknown(3) => {
                let mut coord_seq = CoordSeq::new(1, CoordDimensions::ThreeD)?;
                coord_seq.set_x(0, point.x())?;
                coord_seq.set_y(0, point.y())?;
                coord_seq.set_z(0, point.nth(2).unwrap())?;

                Ok(geos::Geometry::create_point(coord_seq)?)
            }
            _ => Err(geos::Error::GenericError(
                "Unexpected dimension".to_string(),
            )),
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

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.get_x().unwrap(),
            1 => self.0.get_y().unwrap(),
            2 => self.0.get_z().unwrap(),
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl PointTrait for &GEOSPoint {
    type T = f64;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.get_x().unwrap(),
            1 => self.0.get_y().unwrap(),
            2 => self.0.get_z().unwrap(),
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
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

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.get_x().unwrap(),
            1 => self.0.get_y().unwrap(),
            2 => self.0.get_z().unwrap(),
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a> PointTrait for &GEOSConstPoint<'a> {
    type T = f64;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => crate::geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => crate::geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.get_x().unwrap(),
            1 => self.0.get_y().unwrap(),
            2 => self.0.get_z().unwrap(),
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
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
        assert!(point_eq(&scalar, &geos_pt, false))
    }

    #[test]
    fn round_trip_point_z() {
        let arr = point::point_z_array();
        let scalar = arr.value(0);
        let geom = geos::Geometry::try_from(&scalar).unwrap();
        let geos_pt = GEOSPoint::new_unchecked(geom);
        assert!(point_eq(&scalar, &geos_pt, false))
    }
}
