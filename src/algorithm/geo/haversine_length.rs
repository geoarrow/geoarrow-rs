// use crate::GeodesicDistance;
// use crate::{Line, LineString, MultiLineString};

use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use geo::HaversineLength as _HaversineLength;

/// Determine the length of a geometry using the [haversine formula].
///
/// [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula
///
/// *Note*: this implementation uses a mean earth radius of 6371.088 km, based on the [recommendation of
/// the IUGG](ftp://athena.fsv.cvut.cz/ZFG/grs80-Moritz.pdf)
pub trait HaversineLength {
    /// Determine the length of a geometry using the [haversine formula].
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
    /// ]);
    ///
    /// let length = linestring.haversine_length();
    ///
    /// assert_eq!(
    ///     5_570_230., // meters
    ///     length.round()
    /// );
    /// ```
    ///
    /// [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula
    fn haversine_length(&self) -> PrimitiveArray<f64>;
}

impl HaversineLength for PointArray {
    fn haversine_length(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl HaversineLength for MultiPointArray {
    fn haversine_length(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl HaversineLength for LineStringArray {
    fn haversine_length(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.haversine_length())));
        output_array.into()
    }
}

impl HaversineLength for MultiLineStringArray {
    fn haversine_length(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.haversine_length())));
        output_array.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::LineStringArray;
    use arrow2::array::Array;
    use geo::line_string;

    #[test]
    fn haversine_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = input_array.haversine_length();

        // Meters
        let expected = 5_570_230.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
