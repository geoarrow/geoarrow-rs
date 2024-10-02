use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::Float64Array;
use geo::HaversineLength as _HaversineLength;

/// Determine the length of a geometry using the [haversine formula].
///
/// [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula
///
/// *Note*: this implementation uses a mean earth radius of 6371.088 km, based on the [recommendation of
/// the IUGG](ftp://athena.fsv.cvut.cz/ZFG/grs80-Moritz.pdf)
pub trait HaversineLength {
    type Output;

    /// Determine the length of a geometry using the [haversine formula].
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
    /// use geoarrow::algorithm::geo::HaversineLength;
    ///
    /// let linestring = LineString::<f64>::from(vec![
    ///     // New York City
    ///     (-74.006, 40.7128),
    ///     // London
    ///     (-0.1278, 51.5074),
    /// ]);
    /// let linestring_array: LineStringArray<2> = vec![linestring].as_slice().into();
    ///
    /// let length_array = linestring_array.haversine_length();
    ///
    /// assert_eq!(
    ///     5_570_230., // meters
    ///     length_array.value(0).round()
    /// );
    /// ```
    ///
    /// [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula
    fn haversine_length(&self) -> Self::Output;
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl HaversineLength for $type {
            type Output = Float64Array;

            fn haversine_length(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(PointArray<2>);
zero_impl!(MultiPointArray<2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl HaversineLength for $type {
            type Output = Float64Array;

            fn haversine_length(&self) -> Self::Output {
                self.unary_primitive(|geom| geom.to_geo().haversine_length())
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);

impl HaversineLength for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn haversine_length(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().haversine_length(),
            LineString(_, XY) => self.as_line_string::<2>().haversine_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().haversine_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().haversine_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().haversine_length(),
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().haversine_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().haversine_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().haversine_length(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl HaversineLength for ChunkedGeometryArray<PointArray<2>> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn haversine_length(&self) -> Self::Output {
        self.map(|chunk| chunk.haversine_length()).try_into()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl HaversineLength for $type {
            type Output = Result<ChunkedArray<Float64Array>>;

            fn haversine_length(&self) -> Self::Output {
                self.map(|chunk| chunk.haversine_length()).try_into()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<2>>);

impl HaversineLength for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn haversine_length(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().haversine_length(),
            LineString(_, XY) => self.as_line_string::<2>().haversine_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().haversine_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().haversine_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().haversine_length(),
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().haversine_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().haversine_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().haversine_length(),
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
    fn haversine_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray<2> = vec![input_geom].as_slice().into();
        let result_array = input_array.haversine_length();

        // Meters
        let expected = 5_570_230.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
