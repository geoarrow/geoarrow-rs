use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::EuclideanLength as _EuclideanLength;

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
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 116.34),
    /// ];
    /// let linestring_array: LineStringArray<i32, 2> = vec![line_string].as_slice().into();
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

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl EuclideanLength for PointArray<2> {
    type Output = Float64Array;

    fn euclidean_length(&self) -> Self::Output {
        zeroes(self.len(), self.nulls())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> EuclideanLength for $type {
            type Output = Float64Array;

            fn euclidean_length(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(MultiPointArray<O, 2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> EuclideanLength for $type {
            type Output = Float64Array;

            fn euclidean_length(&self) -> Self::Output {
                self.unary_primitive(|geom| geom.to_geo().euclidean_length())
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>);

impl EuclideanLength for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn euclidean_length(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().euclidean_length(),
            LineString(_, XY) => self.as_line_string::<2>().euclidean_length(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().euclidean_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().euclidean_length(),
            // LargePolygon(_, XY) => self.as_large_polygon::<2>().euclidean_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().euclidean_length(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().euclidean_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().euclidean_length(),
            LargeMultiLineString(_, XY) => {
                self.as_large_multi_line_string::<2>().euclidean_length()
            }
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().euclidean_length(),
            // LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().euclidean_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().euclidean_length(),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().euclidean_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().euclidean_length(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().euclidean_length()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl EuclideanLength for ChunkedGeometryArray<PointArray<2>> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn euclidean_length(&self) -> Self::Output {
        self.map(|chunk| chunk.euclidean_length()).try_into()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> EuclideanLength for $type {
            type Output = Result<ChunkedArray<Float64Array>>;

            fn euclidean_length(&self) -> Self::Output {
                self.map(|chunk| chunk.euclidean_length()).try_into()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O, 2>>);

impl EuclideanLength for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn euclidean_length(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().euclidean_length(),
            LineString(_, XY) => self.as_line_string::<2>().euclidean_length(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().euclidean_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().euclidean_length(),
            // LargePolygon(_, XY) => self.as_large_polygon::<2>().euclidean_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().euclidean_length(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().euclidean_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().euclidean_length(),
            LargeMultiLineString(_, XY) => {
                self.as_large_multi_line_string::<2>().euclidean_length()
            }
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().euclidean_length(),
            // LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().euclidean_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().euclidean_length(),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().euclidean_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().euclidean_length(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().euclidean_length()
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
        let input_array: LineStringArray<i64, 2> = vec![input_geom].as_slice().into();
        let result_array = input_array.euclidean_length();

        let expected = 10.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
