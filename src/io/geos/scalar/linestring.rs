use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::GEOSPoint;
use crate::scalar::LineString;
use crate::GeometryArrayTrait;
use arrow2::types::Offset;
use geos::{Geom, GeometryTypes};
use std::iter::Cloned;
use std::slice::Iter;

impl<'b, O: Offset> TryFrom<LineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: LineString<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a LineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a LineString<'_, O>) -> Result<geos::Geometry<'b>> {
        let (start, end) = value.geom_offsets.start_end(value.geom_index);

        let mut sliced_coords = value.coords.clone();
        sliced_coords.to_mut().slice(start, end - start);

        Ok(geos::Geometry::create_line_string(
            sliced_coords.into_owned().try_into()?,
        )?)
    }
}

impl<'b, O: Offset> LineString<'_, O> {
    pub fn to_geos_linear_ring(&self) -> Result<geos::Geometry<'b>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);

        let mut sliced_coords = self.coords.clone();
        sliced_coords.to_mut().slice(start, end - start);

        Ok(geos::Geometry::create_linear_ring(
            sliced_coords.into_owned().try_into()?,
        )?)
    }
}

#[derive(Clone)]
pub struct GEOSLineString<'a>(geos::Geometry<'a>);

impl<'a> GEOSLineString<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::LineString));

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
        Some(GEOSPoint::new_unchecked(point))
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
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}

pub struct GEOSConstLineString<'a, 'b>(geos::ConstGeometry<'a, 'b>);

impl<'a, 'b> GEOSConstLineString<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::LineString));

        Ok(Self(geom))
    }
}

impl<'a, 'b> LineStringTrait<'a> for GEOSConstLineString<'a, 'b> {
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
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a, 'b> LineStringTrait<'a> for &GEOSConstLineString<'a, 'b> {
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
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}
