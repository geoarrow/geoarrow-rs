use crate::algorithm::geo::utils::zeroes;
use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2::types::Offset;
use geo::prelude::Area as GeoArea;

/// Signed and unsigned planar area of a geometry.
///
/// # Examples
///
/// ```
/// use geo::polygon;
/// use geo::Area;
///
/// let mut polygon = polygon![
///     (x: 0., y: 0.),
///     (x: 5., y: 0.),
///     (x: 5., y: 6.),
///     (x: 0., y: 6.),
///     (x: 0., y: 0.),
/// ];
///
/// assert_eq!(polygon.signed_area(), 30.);
/// assert_eq!(polygon.unsigned_area(), 30.);
///
/// polygon.exterior_mut(|line_string| {
///     line_string.0.reverse();
/// });
///
/// assert_eq!(polygon.signed_area(), -30.);
/// assert_eq!(polygon.unsigned_area(), 30.);
/// ```
pub trait Area {
    fn signed_area(&self) -> PrimitiveArray<f64>;

    fn unsigned_area(&self) -> PrimitiveArray<f64>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Area for PointArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: Offset> Area for $type {
            fn signed_area(&self) -> PrimitiveArray<f64> {
                zeroes(self.len(), self.validity())
            }

            fn unsigned_area(&self) -> PrimitiveArray<f64> {
                zeroes(self.len(), self.validity())
            }
        }
    };
}

zero_impl!(LineStringArray<O>);
zero_impl!(MultiPointArray<O>);
zero_impl!(MultiLineStringArray<O>);

macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: Offset> Area for $type {
            fn signed_area(&self) -> PrimitiveArray<f64> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                self.iter_geo()
                    .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
                output_array.into()
            }

            fn unsigned_area(&self) -> PrimitiveArray<f64> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                self.iter_geo()
                    .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(WKBArray<O>);

impl<O: Offset> Area for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn signed_area(&self) -> PrimitiveArray<f64>;

        fn unsigned_area(&self) -> PrimitiveArray<f64>;
    }
}

#[cfg(test)]
mod test {
    use arrow2::array::Float64Array;

    use super::*;
    use crate::test::polygon::polygon_arr;

    #[test]
    fn tmp() {
        let arr = polygon_arr();
        let area = arr.unsigned_area();
        assert_eq!(area, Float64Array::from_vec(vec![28., 18.]));
    }
}
