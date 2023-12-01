use crate::algorithm::geo::utils::zeroes;
use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::prelude::Area as GeoArea;

/// Signed and unsigned planar area of a geometry.
///
/// # Examples
///
/// ```
/// use geo::polygon;
///
/// use geoarrow::algorithm::geo::Area;
/// use geoarrow::array::PolygonArray;
///
/// let polygon = polygon![
///     (x: 0., y: 0.),
///     (x: 5., y: 0.),
///     (x: 5., y: 6.),
///     (x: 0., y: 6.),
///     (x: 0., y: 0.),
/// ];
///
/// let mut reversed_polygon = polygon.clone();
/// reversed_polygon.exterior_mut(|line_string| {
///     line_string.0.reverse();
/// });
///
/// let polygon_array: PolygonArray<i32> = vec![polygon].as_slice().into();
/// let reversed_polygon_array: PolygonArray<i32> = vec![reversed_polygon].as_slice().into();
///
/// assert_eq!(polygon_array.signed_area().value(0), 30.);
/// assert_eq!(polygon_array.unsigned_area().value(0), 30.);
///
/// assert_eq!(reversed_polygon_array.signed_area().value(0), -30.);
/// assert_eq!(reversed_polygon_array.unsigned_area().value(0), 30.);
/// ```
pub trait Area {
    fn signed_area(&self) -> Float64Array;

    fn unsigned_area(&self) -> Float64Array;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Area for PointArray {
    fn signed_area(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }

    fn unsigned_area(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            fn signed_area(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }

            fn unsigned_area(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(LineStringArray<O>);
zero_impl!(MultiPointArray<O>);
zero_impl!(MultiLineStringArray<O>);

macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            fn signed_area(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.signed_area()))
                });
                output_array.finish()
            }

            fn unsigned_area(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.unsigned_area()))
                });
                output_array.finish()
            }
        }
    };
}

iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(WKBArray<O>);

impl<O: OffsetSizeTrait> Area for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn signed_area(&self) -> Float64Array;

        fn unsigned_area(&self) -> Float64Array;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn tmp() {
        let arr = p_array();
        let area = arr.unsigned_area();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
