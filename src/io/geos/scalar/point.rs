use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, PointTrait};
use crate::scalar::Point;
use geos::{CoordDimensions, CoordSeq, Geom, GeometryTypes};

impl<'b> TryFrom<Point<'_>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(value: Point<'_>) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b> TryFrom<&'a Point<'_>> for geos::Geometry<'b> {
    type Error = geos::Error;

    fn try_from(value: &'a Point<'_>) -> std::result::Result<geos::Geometry<'b>, geos::Error> {
        let mut coord_seq = CoordSeq::new(1, CoordDimensions::TwoD)?;
        coord_seq.set_x(0, PointTrait::x(&value))?;
        coord_seq.set_y(0, PointTrait::y(&value))?;

        geos::Geometry::create_point(coord_seq)
    }
}

#[derive(Clone)]
pub struct GEOSPoint<'a>(geos::Geometry<'a>);

impl<'a> GEOSPoint<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Point) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be point".to_string(),
            ))
        }
    }
}

impl<'a> PointTrait for GEOSPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a> PointTrait for &GEOSPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a> CoordTrait for GEOSPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a> CoordTrait for &GEOSPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

pub struct GEOSConstPoint<'a, 'b>(geos::ConstGeometry<'a, 'b>);

impl<'a, 'b> GEOSConstPoint<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::Point) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be point".to_string(),
            ))
        }
    }
}

impl<'a, 'b> PointTrait for GEOSConstPoint<'a, 'b> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a, 'b> PointTrait for &GEOSConstPoint<'a, 'b> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a, 'b> CoordTrait for GEOSConstPoint<'a, 'b> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a, 'b> CoordTrait for &GEOSConstPoint<'a, 'b> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl Clone for GEOSConstPoint<'_, '_> {
    fn clone(&self) -> Self {
        todo!()
    }
}
