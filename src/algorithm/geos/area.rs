use crate::algorithm::geo::utils::zeroes;
use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use crate::error::Result;
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow_array::OffsetSizeTrait;
use geos::Geom;

/// Unsigned planar area of a geometry.
pub trait Area {
    fn area(&self) -> Result<PrimitiveArray<f64>>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Area for PointArray {
    fn area(&self) -> Result<PrimitiveArray<f64>> {
        Ok(zeroes(self.len(), self.validity()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            fn area(&self) -> Result<PrimitiveArray<f64>> {
                Ok(zeroes(self.len(), self.validity()))
            }
        }
    };
}

zero_impl!(LineStringArray<O>);
zero_impl!(MultiPointArray<O>);
zero_impl!(MultiLineStringArray<O>);

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            fn area(&self) -> Result<PrimitiveArray<f64>> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());

                for maybe_g in self.iter_geos() {
                    if let Some(g) = maybe_g {
                        let area = g.area()?;
                        output_array.push(Some(area));
                    } else {
                        output_array.push(None);
                    }
                }

                Ok(output_array.into())
            }
        }
    };
}

iter_geos_impl!(PolygonArray<O>);
iter_geos_impl!(MultiPolygonArray<O>);
iter_geos_impl!(WKBArray<O>);

impl<O: OffsetSizeTrait> Area for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn area(&self) -> Result<PrimitiveArray<f64>>;
    }
}

#[cfg(test)]
mod test {
    use arrow2::array::Float64Array;

    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn tmp() {
        let arr = p_array();
        let area = arr.area().unwrap();
        assert_eq!(area, Float64Array::from_vec(vec![28., 18.]));
    }
}
