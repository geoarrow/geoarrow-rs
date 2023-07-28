use geos::{Geom, GeometryTypes};

use crate::error::Result;
use crate::geo_traits::LineStringTrait;
use crate::io::native::geos::point::GEOSPoint;
use std::borrow::Cow;
use std::iter::Cloned;
use std::slice::Iter;

#[derive(Clone)]
pub struct GEOSLineString<'a>(Cow<'a, geos::Geometry<'a>>);

impl<'a> GEOSLineString<'a> {
    pub fn new_unchecked(geom: Cow<'a, geos::Geometry<'a>>) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: Cow<'a, geos::Geometry<'a>>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::Point));

        Ok(Self(geom))
    }
}

impl<'a> LineStringTrait<'a> for GEOSLineString<'a> {
    type T = f64;
    type ItemType = GEOSPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType> {
        if i > (self.num_coords()) {
            return None;
        }

        let point = self.0.get_point_n(i).unwrap();
        Some(GEOSPoint::new_owned(point))
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> LineStringTrait<'a> for &GEOSLineString<'a> {
    type T = f64;
    type ItemType = GEOSPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType> {
        if i > (self.num_coords()) {
            return None;
        }

        let point = self.0.get_point_n(i).unwrap();
        Some(GEOSPoint::new_owned(point))
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}
