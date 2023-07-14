use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::error::Result;
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2::types::Offset;
use geo::VincentyLength as _VincentyLength;

/// Determine the length of a geometry using [Vincenty’s formulae].
///
/// [Vincenty’s formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
pub trait VincentyLength {
    /// Determine the length of a geometry using [Vincenty’s formulae].
    ///
    /// # Units
    ///
    /// - return value: meters
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::prelude::*;
    /// use geo::LineString;
    ///
    /// let linestring = LineString::<f64>::from(vec![
    ///     // New York City
    ///     (-74.006, 40.7128),
    ///     // London
    ///     (-0.1278, 51.5074),
    ///     // Osaka
    ///     (135.5244559, 34.687455)
    /// ]);
    ///
    /// let length = linestring.vincenty_length().unwrap();
    ///
    /// assert_eq!(
    ///     15_109_158., // meters
    ///     length.round()
    /// );
    /// ```
    ///
    /// [Vincenty’s formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    fn vincenty_length(&self) -> Result<PrimitiveArray<f64>>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl VincentyLength for PointArray {
    fn vincenty_length(&self) -> Result<PrimitiveArray<f64>> {
        Ok(zeroes(self.len(), self.validity()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: Offset> VincentyLength for $type {
            fn vincenty_length(&self) -> Result<PrimitiveArray<f64>> {
                Ok(zeroes(self.len(), self.validity()))
            }
        }
    };
}

zero_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: Offset> VincentyLength for $type {
            fn vincenty_length(&self) -> Result<PrimitiveArray<f64>> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                // TODO: remove unwrap
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push(maybe_g.map(|g| g.vincenty_length().unwrap()))
                });
                Ok(output_array.into())
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
    fn vincenty_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray<i64> = vec![input_geom].into();
        let result_array = input_array.vincenty_length().unwrap();

        // Meters
        let expected = 5585234.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
