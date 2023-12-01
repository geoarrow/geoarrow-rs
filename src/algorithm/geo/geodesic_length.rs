use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
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
    /// use geo::LineString;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::algorithm::geo::GeodesicLength;
    ///
    /// let linestring = LineString::from(vec![
    ///     // New York City
    ///     (-74.006, 40.7128),
    ///     // London
    ///     (-0.1278, 51.5074),
    ///     // Osaka
    ///     (135.5244559, 34.687455)
    /// ]);
    /// let linestring_array: LineStringArray<i32> = vec![linestring].as_slice().into();
    ///
    /// let length_array = linestring_array.geodesic_length();
    ///
    /// assert_eq!(
    ///     15_109_158., // meters
    ///     length_array.value(0).round()
    /// );
    /// ```
    ///
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_length(&self) -> Float64Array;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl GeodesicLength for PointArray {
    fn geodesic_length(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            fn geodesic_length(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            fn geodesic_length(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.geodesic_length()))
                });
                output_array.finish()
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
    fn geodesic_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
            // Osaka
            (x: 135.5244559, y: 34.687455),
        ];
        let input_array: LineStringArray<i64> = vec![input_geom].as_slice().into();
        let result_array = input_array.geodesic_length();

        // Meters
        let expected = 15_109_158.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
