use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
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

impl EuclideanLength for PointArray {
    fn euclidean_length(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl EuclideanLength for MultiPointArray {
    fn euclidean_length(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl EuclideanLength for LineStringArray {
    fn euclidean_length(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
        output_array.into()
    }
}

impl EuclideanLength for MultiLineStringArray {
    fn euclidean_length(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
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
    fn euclidean_length_geoarrow_linestring() {
        let input_geom = line_string![
            (x: 1., y: 1.),
            (x: 7., y: 1.),
            (x: 8., y: 1.),
            (x: 9., y: 1.),
            (x: 10., y: 1.),
            (x: 11., y: 1.)
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = input_array.euclidean_length();

        let expected = 10.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
