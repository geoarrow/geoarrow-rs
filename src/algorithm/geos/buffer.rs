use crate::array::{PointArray, PolygonArray, PolygonBuilder};
use crate::error::Result;
use crate::io::geos::scalar::GEOSPolygon;
use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};
use arrow_array::OffsetSizeTrait;
use geos::{BufferParams, Geom};

pub trait Buffer<O: OffsetSizeTrait> {
    type Output;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output;

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams<'_>) -> Self::Output;
}

impl<O: OffsetSizeTrait> Buffer<O> for PointArray {
    type Output = Result<PolygonArray<O>>;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output {
        let mut builder = PolygonBuilder::new();

        for maybe_g in self.iter() {
            if let Some(g) = maybe_g {
                let x = g.to_geos()?.buffer(width, quadsegs)?;
                let polygon = GEOSPolygon::new_unchecked(x);
                builder.push_polygon(Some(&polygon))?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
    }

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams<'_>) -> Self::Output {
        let mut builder = PolygonBuilder::new();

        for maybe_g in self.iter() {
            if let Some(g) = maybe_g {
                let x = g.to_geos()?.buffer_with_params(width, buffer_params)?;
                let polygon = GEOSPolygon::new_unchecked(x);
                builder.push_polygon(Some(&polygon))?;
            } else {
                builder.push_null();
            }
        }

        Ok(builder.finish())
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
        let buffered: PolygonArray<i32> = arr.buffer(1., 8).unwrap();
        dbg!(buffered);
    }
}
