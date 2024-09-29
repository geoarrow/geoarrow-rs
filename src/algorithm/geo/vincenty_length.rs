use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::Float64Array;
use geo::VincentyLength as _VincentyLength;

/// Determine the length of a geometry using [Vincenty’s formulae].
///
/// [Vincenty’s formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
pub trait VincentyLength {
    type Output;

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
    /// let linestring_array: LineStringArray<2> = vec![linestring].as_slice().into();
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
    fn vincenty_length(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl VincentyLength for PointArray<2> {
    type Output = Result<Float64Array>;

    fn vincenty_length(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl VincentyLength for $type {
            type Output = Result<Float64Array>;

            fn vincenty_length(&self) -> Self::Output {
                Ok(zeroes(self.len(), self.nulls()))
            }
        }
    };
}

zero_impl!(MultiPointArray<2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl VincentyLength for $type {
            type Output = Result<Float64Array>;

            fn vincenty_length(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geo().vincenty_length())?)
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);

impl VincentyLength for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn vincenty_length(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().vincenty_length(),
            LineString(_, XY) => self.as_line_string::<2>().vincenty_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().vincenty_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().vincenty_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().vincenty_length(),
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().vincenty_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().vincenty_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().vincenty_length(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl VincentyLength for ChunkedGeometryArray<PointArray<2>> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn vincenty_length(&self) -> Self::Output {
        self.try_map(|chunk| chunk.vincenty_length())?.try_into()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl VincentyLength for $type {
            type Output = Result<ChunkedArray<Float64Array>>;

            fn vincenty_length(&self) -> Self::Output {
                self.try_map(|chunk| chunk.vincenty_length())?.try_into()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<2>>);

impl VincentyLength for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn vincenty_length(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().vincenty_length(),
            LineString(_, XY) => self.as_line_string::<2>().vincenty_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().vincenty_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().vincenty_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().vincenty_length(),
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().vincenty_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().vincenty_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().vincenty_length(),
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
    fn vincenty_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray<2> = vec![input_geom].as_slice().into();
        let result_array = input_array.vincenty_length().unwrap();

        // Meters
        let expected = 5585234.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
