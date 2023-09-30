use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PolygonTrait;
use crate::scalar::Polygon;
use arrow2::types::Offset;
use geos::{Geom, GeometryTypes};

impl<'b, O: Offset> TryFrom<Polygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: Polygon<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a Polygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a Polygon<'_, O>) -> Result<geos::Geometry<'b>> {
        if let Some(exterior) = value.exterior() {
            let exterior = exterior.to_geos_linear_ring()?;
            let num_interiors = value.num_interiors();

            let mut interiors = Vec::with_capacity(num_interiors);

            for interior_idx in 0..num_interiors {
                let interior = value.interior(interior_idx).unwrap();
                interiors.push(interior.to_geos_linear_ring()?);
            }

            Ok(geos::Geometry::create_polygon(exterior, interiors)?)
        } else {
            Ok(geos::Geometry::create_empty_polygon()?)
        }
    }
}

#[derive(Clone)]
pub struct GEOSPolygon<'a>(pub(crate) geos::Geometry<'a>);

impl<'a> GEOSPolygon<'a> {
    pub fn new_unchecked(geom: geos::Geometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry<'a>) -> Result<Self> {
        // TODO: make Err
        assert!(matches!(geom.geometry_type(), GeometryTypes::LineString));

        Ok(Self(geom))
    }
}

// impl<'a> PolygonTrait<'a> for GEOSPolygon<'a> {
//     type T = f64;
//     type ItemType = GEOSLineString<'a>;
//     type Iter = Cloned<Iter<'a, Self::ItemType>>;

//     fn num_interiors(&self) -> usize {
//         self.0.get_num_interior_rings().unwrap()
//     }

//     fn exterior(&self) -> Option<Self::ItemType> {
//         let ring = self.0.get_exterior_ring().unwrap();
//         ring.
//     }

//     fn num_coords(&self) -> usize {
//         self.0.get_num_points().unwrap()
//     }

//     fn coord(&self, i: usize) -> Option<Self::ItemType> {
//         if i > (self.num_coords()) {
//             return None;
//         }

//         let point = self.0.get_point_n(i).unwrap();
//         Some(GEOSLineString::new_unchecked(point))
//     }

//     fn coords(&'a self) -> Self::Iter {
//         todo!()
//     }
// }

// impl<'a> PolygonTrait<'a> for &GEOSPolygon<'a> {
//     type T = f64;
//     type ItemType = GEOSPoint<'a>;
//     type Iter = Cloned<Iter<'a, Self::ItemType>>;

//     fn num_coords(&self) -> usize {
//         self.0.get_num_points().unwrap()
//     }

//     fn coord(&self, i: usize) -> Option<Self::ItemType> {
//         if i > (self.num_coords()) {
//             return None;
//         }

//         let point = self.0.get_point_n(i).unwrap();
//         Some(GEOSPoint::new_unchecked(point))
//     }

//     fn coords(&'a self) -> Self::Iter {
//         todo!()
//     }
// }
