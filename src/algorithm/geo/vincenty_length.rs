use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::error::Result;
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
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
    /// use geo::LineString;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::algorithm::geo::VincentyLength;
    ///
    /// let linestring = LineString::<f64>::from(vec![
    ///     // New York City
    ///     (-74.006, 40.7128),
    ///     // London
    ///     (-0.1278, 51.5074),
    ///     // Osaka
    ///     (135.5244559, 34.687455)
    /// ]);
    /// let linestring_array: LineStringArray<i32> = vec![linestring].as_slice().into();
    ///
    /// let length_array = linestring_array.vincenty_length().unwrap();
    ///
    /// assert_eq!(
    ///     15_109_158., // meters
    ///     length_array.value(0).round()
    /// );
    /// ```
    ///
    /// [Vincenty’s formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    fn vincenty_length(&self) -> Result<Float64Array>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl VincentyLength for PointArray {
    fn vincenty_length(&self) -> Result<Float64Array> {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> VincentyLength for $type {
            fn vincenty_length(&self) -> Result<Float64Array> {
                Ok(zeroes(self.len(), self.nulls()))
            }
        }
    };
}

zero_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> VincentyLength for $type {
            fn vincenty_length(&self) -> Result<Float64Array> {
                let mut output_array = Float64Builder::with_capacity(self.len());
                // TODO: remove unwrap
                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.vincenty_length().unwrap()))
                });
                Ok(output_array.finish())
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
    use arrow_array::Array;
    use geo::line_string;

    #[test]
    fn vincenty_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray<i64> = vec![input_geom].as_slice().into();
        let result_array = input_array.vincenty_length().unwrap();

        // Meters
        let expected = 5585234.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
