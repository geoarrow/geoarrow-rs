use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPointTrait;
use crate::scalar::MultiPoint;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<MultiPoint<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPoint<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a MultiPoint<'_, O>> for geos::Geometry<'b> {
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

// Hit lifetime issues with the multi geometries because get_geometry_n returns a ConstGeometry

// #[derive(Clone)]
// pub struct GEOSMultiPoint<'a>(geos::Geometry<'a>);

// impl<'a> GEOSMultiPoint<'a> {
//     pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
//         Self(geom)
//     }

//     pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
//         // TODO: make Err
//         assert!(matches!(geom.geometry_type(), GeometryTypes::MultiPoint));

//         Ok(Self(geom))
//     }
// }

// impl<'a> MultiPointTrait<'a> for GEOSMultiPoint<'a> {
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
//         Some(GEOSConstPoint::new_unchecked(point))
//     }

//     fn points(&'a self) -> Self::Iter {
//         todo!()
//     }
// }

// impl<'a> MultiPointTrait<'a> for &GEOSMultiPoint<'a> {
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
//         Some(GEOSConstPoint::new_unchecked(point))
//     }

//     fn points(&'a self) -> Self::Iter {
//         todo!()
//     }
// }
