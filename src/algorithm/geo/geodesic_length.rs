use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow_array::OffsetSizeTrait;
use geo::GeodesicLength as _GeodesicLength;

/// Determine the length of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)]. As opposed to older methods
/// like Vincenty, this method is accurate to a few nanometers and always converges.
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
pub trait GeodesicLength {
    /// Determine the length of a geometry on an ellipsoidal model of the earth.
    ///
    /// This uses the geodesic measurement methods given by [Karney (2013)]. As opposed to older methods
    /// like Vincenty, this method is accurate to a few nanometers and always converges.
    ///
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
    /// let linestring = LineString::from(vec![
    ///     // New York City
    ///     (-74.006, 40.7128),
    ///     // London
    ///     (-0.1278, 51.5074),
    ///     // Osaka
    ///     (135.5244559, 34.687455)
    /// ]);
    ///
    /// let length = linestring.geodesic_length();
    ///
    /// assert_eq!(
    ///     15_109_158., // meters
    ///     length.round()
    /// );
    /// ```
    ///
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_length(&self) -> PrimitiveArray<f64>;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl GeodesicLength for PointArray {
    fn geodesic_length(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            fn geodesic_length(&self) -> PrimitiveArray<f64> {
                zeroes(self.len(), self.validity())
            }
        }
    };
}

zero_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            fn geodesic_length(&self) -> PrimitiveArray<f64> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                self.iter_geo()
                    .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.geodesic_length())));
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
    fn geodesic_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
            // Osaka
            (x: 135.5244559, y: 34.687455),
        ];
        let input_array: LineStringArray<i64> = vec![input_geom].into();
        let result_array = input_array.geodesic_length();

        // Meters
        let expected = 15_109_158.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
