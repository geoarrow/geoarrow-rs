use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPointTrait;
use crate::io::geos::scalar::GEOSConstPoint;
use crate::scalar::MultiPoint;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};
use std::slice::Iter;

impl<'b, O: OffsetSizeTrait> TryFrom<MultiPoint<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPoint<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a MultiPoint<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a MultiPoint<'_, O>) -> Result<geos::Geometry<'b>> {
        let num_points = value.num_points();
        let mut geos_geoms = Vec::with_capacity(num_points);

        for point_idx in 0..num_points {
            let point = value.point(point_idx).unwrap();
            geos_geoms.push(point.try_into()?);
        }

        Ok(geos::Geometry::create_multipoint(geos_geoms)?)
    }
}

#[derive(Clone)]
pub struct GEOSMultiPoint<'a>(pub(crate) geos::Geometry<'a>);

impl<'a> GEOSMultiPoint<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::MultiPoint) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be multi point".to_string(),
            ))
        }
    }

    pub fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }
}

impl<'a> MultiPointTrait for GEOSMultiPoint<'a> {
    type T = f64;
    type ItemType<'b, 'c: 'a> = GEOSConstPoint<'a, 'c> where Self: 'b;
    type Iter<'b, 'c> = GEOSMultiPointIterator<'a> where Self: 'b;

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        if i > self.num_points() {
            return None;
        }

        let point = self.0.get_geometry_n(i).unwrap();
        let x = GEOSConstPoint::new_unchecked(point);
        Some(x)
    }

    fn points(&self) -> Self::Iter<'_> {
        todo!()
    }
}

#[derive(Clone)]
pub struct GEOSMultiPointIterator<'a> {
    geom: &'a GEOSMultiPoint<'a>,
    index: usize,
    end: usize,
}

impl<'a> GEOSMultiPointIterator<'a> {
    #[inline]
    pub fn new(geom: &'a GEOSMultiPoint<'a>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_points(),
        }
    }
}

impl<'a> Iterator for GEOSMultiPointIterator<'a> {
    type Item<'b> = GEOSConstPoint<'a, 'b>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.point(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

impl<'a> ExactSizeIterator for GEOSMultiPointIterator<'a> {}

// NOTE: the MultiPoint traits aren't implemented because get_geometry_n returns a ConstGeometry,
// which then has _two_ lifetime parameters, and it looks like a total black hole to get that
// working with these traits.

// impl<'a> MultiPointTrait for GEOSMultiPoint<'a> {
//     type T = f64;
//     type ItemType = GEOSConstPoint<'a, 'a>;
//     type Iter = Cloned<Iter<'a, Self::ItemType>>;

//     fn num_points(&self) -> usize {
//         self.0.get_num_geometries().unwrap()
//     }

//     fn point(&self, i: usize) -> Option<Self::ItemType> {
//         if i > (self.num_points()) {
//             return None;
//         }

//         let point = self.0.get_geometry_n(i).unwrap();
//         Some(GEOSConstPoint::new_unchecked(&point))
//     }

//     fn points(&'a self) -> Self::Iter {
//         todo!()
//     }
// }

// impl<'a> MultiPointTrait for &GEOSMultiPoint<'a> {
//     type T = f64;
//     type ItemType = GEOSConstPoint<'a, 'a>;
//     type Iter = Cloned<Iter<'a, Self::ItemType>>;

//     fn num_points(&self) -> usize {
//         self.0.get_num_geometries().unwrap()
//     }

//     fn point(&self, i: usize) -> Option<Self::ItemType> {
//         if i > (self.num_points()) {
//             return None;
//         }

//         let point = self.0.get_geometry_n(i).unwrap();
//         Some(GEOSConstPoint::new_unchecked(&point))
//     }

//     fn points(&'a self) -> Self::Iter {
//         todo!()
//     }
// }
