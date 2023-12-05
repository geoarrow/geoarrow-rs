use crate::algorithm::geo::utils::zeroes;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WKBArray,
};
use crate::error::Result;
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geos::Geom;

/// Unsigned planar area of a geometry.
pub trait Area {
    fn area(&self) -> Result<Float64Array>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Area for PointArray {
    fn area(&self) -> Result<Float64Array> {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            fn area(&self) -> Result<Float64Array> {
                Ok(zeroes(self.len(), self.nulls()))
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
            fn area(&self) -> Result<Float64Array> {
                let mut output_array = Float64Builder::with_capacity(self.len());

                for maybe_g in self.iter_geos() {
                    if let Some(g) = maybe_g {
                        let area = g.area()?;
                        output_array.append_value(area);
                    } else {
                        output_array.append_null();
                    }
                }

                Ok(output_array.finish())
            }
        }
    };
}

iter_geos_impl!(PolygonArray<O>);
iter_geos_impl!(MultiPolygonArray<O>);
iter_geos_impl!(WKBArray<O>);

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn tmp() {
        let arr = p_array();
        let area = arr.area().unwrap();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
