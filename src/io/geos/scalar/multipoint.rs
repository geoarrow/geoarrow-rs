use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPointTrait;
use crate::io::geos::scalar::GEOSConstPoint;
use crate::scalar::MultiPoint;
use arrow_array::OffsetSizeTrait;
use geos::{Geom, GeometryTypes};

impl<O: OffsetSizeTrait> TryFrom<MultiPoint<'_, O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: MultiPoint<'_, O>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<&'a MultiPoint<'_, O>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a MultiPoint<'_, O>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_multipoint(
            value
                .points()
                .map(|points| points.try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}

#[derive(Clone)]
pub struct GEOSMultiPoint(pub(crate) geos::Geometry);

impl GEOSMultiPoint {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
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

impl MultiPointTrait for GEOSMultiPoint {
    type T = f64;
    type ItemType<'a> = GEOSConstPoint<'a> where Self: 'a;

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}

impl MultiPointTrait for &GEOSMultiPoint {
    type T = f64;
    type ItemType<'a> = GEOSConstPoint<'a> where Self: 'a;

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}

// NOTE: the MultiPoint traits aren't implemented because get_geometry_n returns a ConstGeometry,
// which then has _two_ lifetime parameters, and it looks like a total black hole to get that
// working with these traits.

// impl<'a> MultiPointTrait for GEOSMultiPoint<'a> {
//     type T = f64;
//     type ItemType = GEOSConstPoint<'a, 'a>;

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
// }

// impl<'a> MultiPointTrait for &GEOSMultiPoint<'a> {
//     type T = f64;
//     type ItemType = GEOSConstPoint<'a, 'a>;

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
// }
