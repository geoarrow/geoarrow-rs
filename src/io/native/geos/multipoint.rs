use crate::error::Result;
use crate::geo_traits::MultiPointTrait;
use crate::io::native::geos::point::GEOSBorrowedPoint;
use geos::{Geom, GeometryTypes};
use std::iter::Cloned;
use std::slice::Iter;

#[derive(Clone)]
pub struct GEOSMultiPoint<'a, 'b>(&'b geos::Geometry<'a>);

impl<'a, 'b> GEOSMultiPoint<'a, 'b> {
    #[allow(dead_code)]
    pub fn try_new(geom: &'a geos::Geometry<'a>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::MultiPoint));

        Ok(Self(geom))
    }
}

impl<'a, 'b: 'a> MultiPointTrait<'a> for GEOSMultiPoint<'a, 'b> {
    type T = f64;
    type ItemType = GEOSBorrowedPoint<'a, 'b>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_points() {
            return None;
        }

        let point = self.0.get_geometry_n(i).unwrap();
        Some(GEOSBorrowedPoint::new_unchecked(point))
    }

    fn points(&'a self) -> Self::Iter {
        todo!()
    }
}

// impl<'b, 'a> MultiPointTrait<'a> for &GEOSMultiPoint<'a,'b> {
//     type T = f64;
//     type ItemType = GEOSBorrowedPoint<'a, 'b>;
//     type Iter = Cloned<Iter<'b, Self::ItemType>>;

//     fn num_points(&self) -> usize {
//         self.0.get_num_geometries().unwrap()
//     }

//     fn point(&self, i: usize) -> Option<Self::ItemType> {
//         if i > self.num_points() {
//             return None;
//         }

//         let point = self.0.get_geometry_n(i).unwrap();
//         Some(GEOSBorrowedPoint::new_unchecked(point))
//     }

//     fn points(&'a self) -> Self::Iter {
//         todo!()
//     }
// }
