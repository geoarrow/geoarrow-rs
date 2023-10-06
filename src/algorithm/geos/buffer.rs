use crate::array::{PointArray, PolygonArray};
use crate::error::Result;
use crate::GeometryArrayTrait;
use geos::Geom;

pub trait Buffer {
    type Output;

    fn buffer(&self, width: f64, quadsegs: i32) -> Result<Self::Output>;
}

impl Buffer for PointArray {
    type Output = PolygonArray<i32>;

    fn buffer(&self, width: f64, quadsegs: i32) -> Result<Self::Output> {
        // NOTE: the bumpalo allocator didn't appear to make any perf difference with geos :shrug:
        // Presumably GEOS is allocating on its own before we can put the geometry in the Bump?
        let bump = bumpalo::Bump::new();

        let mut geos_geoms = bumpalo::collections::Vec::with_capacity_in(self.len(), &bump);

        for maybe_g in self.iter_geos() {
            if let Some(g) = maybe_g {
                let area = g.buffer(width, quadsegs)?;
                geos_geoms.push(Some(area));
            } else {
                geos_geoms.push(None);
            }
        }

        let polygon_array: PolygonArray<i32> = geos_geoms.try_into()?;
        Ok(polygon_array)
    }
}

// // Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
// impl Area for PointArray {
//     fn area(&self) -> Result<PrimitiveArray<f64>> {
//         Ok(zeroes(self.len(), self.nulls()))
//     }
// }

// /// Implementation where the result is zero.
// macro_rules! zero_impl {
//     ($type:ty) => {
//         impl<O: OffsetSizeTrait> Area for $type {
//             fn area(&self) -> Result<PrimitiveArray<f64>> {
//                 Ok(zeroes(self.len(), self.nulls()))
//             }
//         }
//     };
// }

// zero_impl!(LineStringArray<O>);
// zero_impl!(MultiPointArray<O>);
// zero_impl!(MultiLineStringArray<O>);

// macro_rules! iter_geos_impl {
//     ($type:ty) => {
//         impl<O: OffsetSizeTrait> Area for $type {
//             fn area(&self) -> Result<PrimitiveArray<f64>> {
//             }
//         }
//     };
// }

// iter_geos_impl!(PolygonArray<O>);
// iter_geos_impl!(MultiPolygonArray<O>);
// iter_geos_impl!(WKBArray<O>);

// impl<O: OffsetSizeTrait> Area for GeometryArray<O> {
//     crate::geometry_array_delegate_impl! {
//         fn area(&self) -> Result<PrimitiveArray<f64>>;
//     }
// }

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;

    #[test]
    fn point_buffer() {
        let arr = point_array();
        let buffered = arr.buffer(1., 8).unwrap();
        dbg!(buffered);
    }
}
