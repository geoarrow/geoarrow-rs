use std::borrow::Cow;

use geos::{Geom, GeometryTypes};

use crate::error::Result;
use crate::geo_traits::{CoordTrait, PointTrait};

#[derive(Clone)]
pub struct GEOSPoint<'a>(Cow<'a, geos::Geometry<'a>>);

impl<'a> GEOSPoint<'a> {
    // pub fn new_unchecked(geom: &'a geos::Geometry<'a>) -> Self {
    //     Self(geom)
    // }

    // pub fn try_new(geom: &'a geos::Geometry<'a>) -> Result<Self> {
    //     // TODO: make Err
    //     assert!(matches!(geom.geometry_type(), GeometryTypes::Point));

    //     Ok(Self(geom))
    // }

    pub fn new_owned(geom: geos::Geometry<'a>) -> Self {
        Self(Cow::Owned(geom))
    }

    pub fn new_borrowed(geom: &'a geos::Geometry<'a>) -> Self {
        Self(Cow::Borrowed(geom))
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

pub struct GEOSBorrowedPoint<'a, 'b>(geos::ConstGeometry<'a, 'b>);

// TODO:
impl<'a, 'b> Clone for GEOSBorrowedPoint<'a, 'b> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<'a, 'b> GEOSBorrowedPoint<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::Point));

        Ok(Self(geom))
    }
}

impl<'a, 'b> PointTrait for GEOSBorrowedPoint<'a, 'b> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}

impl<'a, 'b> PointTrait for &GEOSBorrowedPoint<'a, 'b> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.0.get_x().unwrap()
    }

    fn y(&self) -> Self::T {
        self.0.get_y().unwrap()
    }
}
