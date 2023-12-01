use crate::array::util::OffsetBufferUtils;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::GEOSPoint;
use crate::scalar::LineString;
use crate::trait_::GeometryArraySelfMethods;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};
use std::iter::Cloned;
use std::slice::Iter;

impl<'b, O: OffsetSizeTrait> TryFrom<LineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: LineString<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a LineString<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a LineString<'_, O>) -> Result<geos::Geometry<'b>> {
        let (start, end) = value.geom_offsets.start_end(value.geom_index);

        let sliced_coords = value.coords.clone().to_mut().slice(start, end - start);

        Ok(geos::Geometry::create_line_string(
            sliced_coords.try_into()?,
        )?)
    }
}

impl<'b, O: OffsetSizeTrait> LineString<'_, O> {
    pub fn to_geos_linear_ring(&self) -> Result<geos::Geometry<'b>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);

        let sliced_coords = self.coords.clone().to_mut().slice(start, end - start);

        Ok(geos::Geometry::create_linear_ring(
            sliced_coords.try_into()?,
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
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > (self.num_coords()) {
            return None;
        }

        let point = self.0.get_point_n(i).unwrap();
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&self) -> Self::Iter<'_> {
        todo!()
    }
}

impl<'a> LineStringTrait for &'a GEOSLineString<'a> {
    type T = f64;
    type ItemType<'b> = GEOSPoint<'a> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > (self.num_coords()) {
            return None;
        }

        let point = self.0.get_point_n(i).unwrap();
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&self) -> Self::Iter<'_> {
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
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

// TODO: uncomment

// error[E0477]: the type `io::geos::scalar::linestring::GEOSConstLineString<'a, 'b>` does not fulfill the required lifetime
//    --> src/io/geos/scalar/linestring.rs:147:25
//     |
// 147 |     fn coords(&self) -> Self::Iter<'_> {
//     |                         ^^^^^^^^^^^^^^
//     |
// note: type must outlive the lifetime `'a` as defined here as required by this binding
//    --> src/io/geos/scalar/linestring.rs:129:6
//     |
// 129 | impl<'a, 'b> LineStringTrait for GEOSConstLineString<'a, 'b> {
//     |      ^^

impl<'a, 'b> LineStringTrait for GEOSConstLineString<'a, 'b> {
    type T = f64;
    type ItemType<'c> = GEOSPoint<'a> where Self: 'c;
    type Iter<'c> = Cloned<Iter<'c, Self::ItemType<'c>>> where Self: 'c;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > (self.num_coords()) {
            return None;
        }

        let point = self.0.get_point_n(i).unwrap();
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&self) -> Self::Iter<'_> {
        todo!()
    }
}

impl<'a, 'b> LineStringTrait for &'a GEOSConstLineString<'a, 'b> {
    type T = f64;
    type ItemType<'c> = GEOSPoint<'a> where Self: 'c;
    type Iter<'c> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'c;

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > (self.num_coords()) {
            return None;
        }

        let point = self.0.get_point_n(i).unwrap();
        Some(GEOSPoint::new_unchecked(point))
    }

    fn coords(&self) -> Self::Iter<'_> {
        todo!()
    }
}
