use crate::array::{PointArray, PolygonArray, PolygonBuilder};
use crate::error::Result;
use crate::io::geos::scalar::GEOSPolygon;
use crate::trait_::{ArrayAccessor, NativeScalar};
use geos::{BufferParams, Geom};

pub trait Buffer {
    type Output;

    fn buffer(&self, width: f64, quadsegs: i32) -> Self::Output;

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output;
}

impl Buffer for PointArray<2> {
    type Output = Result<PolygonArray<2>>;

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

    fn buffer_with_params(&self, width: f64, buffer_params: &BufferParams) -> Self::Output {
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
// impl Area for PointArray<2> {
//     fn area(&self) -> Result<PrimitiveArray<f64>> {
//         Ok(zeroes(self.len(), self.nulls()))
//     }
// }

// /// Implementation where the result is zero.
// macro_rules! zero_impl {
//     ($type:ty) => {
//         impl Area for $type {
//             fn area(&self) -> Result<PrimitiveArray<f64>> {
//                 Ok(zeroes(self.len(), self.nulls()))
//             }
//         }
//     };
// }

// zero_impl!(LineStringArray<2>);
// zero_impl!(MultiPointArray<2>);
// zero_impl!(MultiLineStringArray<2>);

// macro_rules! iter_geos_impl {
//     ($type:ty) => {
//         impl<O: OffsetSizeTrait> Area for $type {
//             fn area(&self) -> Result<PrimitiveArray<f64>> {
//             }
//         }
//     };
// }

// iter_geos_impl!(PolygonArray<2>);
// iter_geos_impl!(MultiPolygonArray<2>);
// iter_geos_impl!(WKBArray<O>);

// impl<O: OffsetSizeTrait> Area for GeometryArray<2> {
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
        let buffered: PolygonArray<2> = arr.buffer(1., 8).unwrap();
        dbg!(buffered);
    }
}
