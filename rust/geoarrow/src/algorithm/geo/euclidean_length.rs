use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::Float64Array;
// use geo::line_measures::LengthMeasurable;
use geo::{Euclidean, Length};

pub trait EuclideanLength {
    type Output;

    /// Calculation of the length of a Line
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::line_string;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::algorithm::geo::EuclideanLength;
    /// use geoarrow_schema::Dimension;
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 116.34),
    /// ];
    /// let linestring_array: LineStringArray = (vec![line_string].as_slice(), Dimension::XY).into();
    ///
    /// let length_array = linestring_array.euclidean_length();
    ///
    /// assert_eq!(
    ///     2.,
    ///     length_array.value(0),
    /// )
    /// ```
    fn euclidean_length(&self) -> Self::Output;
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl EuclideanLength for $type {
            type Output = Float64Array;

            fn euclidean_length(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(PointArray);
zero_impl!(MultiPointArray);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl EuclideanLength for $type {
            type Output = Float64Array;

            fn euclidean_length(&self) -> Self::Output {
                self.unary_primitive(|geom| Euclidean.length(&geom.to_geo()))
            }
        }
    };
}

iter_geo_impl!(LineStringArray);
iter_geo_impl!(MultiLineStringArray);

impl EuclideanLength for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn euclidean_length(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => self.as_point().euclidean_length(),
            LineString(_) => self.as_line_string().euclidean_length(),
            // Polygon(_) => self.as_polygon().euclidean_length(),
            MultiPoint(_) => self.as_multi_point().euclidean_length(),
            MultiLineString(_) => self.as_multi_line_string().euclidean_length(),
            // MultiPolygon(_) => self.as_multi_polygon().euclidean_length(),
            // Mixed(_) => self.as_mixed().euclidean_length(),
            // GeometryCollection(_) => self.as_geometry_collection().euclidean_length(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl EuclideanLength for ChunkedGeometryArray<PointArray> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn euclidean_length(&self) -> Self::Output {
        self.map(|chunk| chunk.euclidean_length()).try_into()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl EuclideanLength for $type {
            type Output = Result<ChunkedArray<Float64Array>>;

            fn euclidean_length(&self) -> Self::Output {
                self.map(|chunk| chunk.euclidean_length()).try_into()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray>);

impl EuclideanLength for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn euclidean_length(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().euclidean_length(),
            LineString(_) => self.as_line_string().euclidean_length(),
            // Polygon(_) => self.as_polygon().euclidean_length(),
            // LargePolygon(_) => self.as_large_polygon().euclidean_length(),
            MultiPoint(_) => self.as_multi_point().euclidean_length(),
            MultiLineString(_) => self.as_multi_line_string().euclidean_length(),
            // MultiPolygon(_) => self.as_multi_polygon().euclidean_length(),
            // LargeMultiPolygon(_) => self.as_large_multi_polygon().euclidean_length(),
            // Mixed(_) => self.as_mixed().euclidean_length(),
            // LargeMixed(_) => self.as_large_mixed().euclidean_length(),
            // GeometryCollection(_) => self.as_geometry_collection().euclidean_length(),
            // LargeGeometryCollection(_) => {
            //     self.as_large_geometry_collection().euclidean_length()
            // }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::LineStringArray;
    use arrow_array::Array;
    use geo::line_string;
    use geoarrow_schema::Dimension;

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
        let input_array: LineStringArray = (vec![input_geom].as_slice(), Dimension::XY).into();
        let result_array = input_array.euclidean_length();

        let expected = 10.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
