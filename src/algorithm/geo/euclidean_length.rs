use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2::types::Offset;
use geo::EuclideanLength as _EuclideanLength;

pub trait EuclideanLength {
    /// Calculation of the length of a Line
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::EuclideanLength;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 116.34),
    /// ];
    ///
    /// assert_eq!(
    ///     2.,
    ///     line_string.euclidean_length(),
    /// )
    /// ```
    fn euclidean_length(&self) -> PrimitiveArray<f64>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl EuclideanLength for PointArray {
    fn euclidean_length(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: Offset> EuclideanLength for $type {
            fn euclidean_length(&self) -> PrimitiveArray<f64> {
                zeroes(self.len(), self.validity())
            }
        }
    };
}

zero_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: Offset> EuclideanLength for $type {
            fn euclidean_length(&self) -> PrimitiveArray<f64> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                self.iter_geo()
                    .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::LineStringArray;
    use arrow2::array::Array;
    use geo::line_string;

    #[test]
    fn euclidean_length_geoarrow_linestring() {
        let input_geom = line_string![
            (x: 1., y: 1.),
            (x: 7., y: 1.),
            (x: 8., y: 1.),
            (x: 9., y: 1.),
            (x: 10., y: 1.),
            (x: 11., y: 1.)
        ];
        let input_array: LineStringArray<i64> = vec![input_geom].into();
        let result_array = input_array.euclidean_length();

        let expected = 10.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
