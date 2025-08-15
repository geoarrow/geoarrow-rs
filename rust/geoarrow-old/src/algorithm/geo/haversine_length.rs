use crate::NativeArray;
use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use arrow_array::Float64Array;
use geo::{Haversine, Length};

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
    /// use geoarrow_schema::Dimension;
    ///
    /// let linestring = LineString::<f64>::from(vec![
    ///     // New York City
    ///     (-74.006, 40.7128),
    ///     // London
    ///     (-0.1278, 51.5074),
    /// ]);
    /// let linestring_array: LineStringArray = (vec![linestring].as_slice(), Dimension::XY).into();
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

zero_impl!(PointArray);
zero_impl!(MultiPointArray);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl HaversineLength for $type {
            type Output = Float64Array;

            fn haversine_length(&self) -> Self::Output {
                self.unary_primitive(|geom| Haversine.length(&geom.to_geo()))
            }
        }
    };
}

iter_geo_impl!(LineStringArray);
iter_geo_impl!(MultiLineStringArray);

impl HaversineLength for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn haversine_length(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => self.as_point().haversine_length(),
            LineString(_) => self.as_line_string().haversine_length(),
            // Polygon(_) => self.as_polygon().haversine_length(),
            MultiPoint(_) => self.as_multi_point().haversine_length(),
            MultiLineString(_) => self.as_multi_line_string().haversine_length(),
            // MultiPolygon(_) => self.as_multi_polygon().haversine_length(),
            // Mixed(_) => self.as_mixed().haversine_length(),
            // GeometryCollection(_) => self.as_geometry_collection().haversine_length(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl HaversineLength for ChunkedGeometryArray<PointArray> {
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

chunked_impl!(ChunkedGeometryArray<LineStringArray>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray>);

impl HaversineLength for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn haversine_length(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().haversine_length(),
            LineString(_) => self.as_line_string().haversine_length(),
            // Polygon(_) => self.as_polygon().haversine_length(),
            MultiPoint(_) => self.as_multi_point().haversine_length(),
            MultiLineString(_) => self.as_multi_line_string().haversine_length(),
            // MultiPolygon(_) => self.as_multi_polygon().haversine_length(),
            // Mixed(_) => self.as_mixed().haversine_length(),
            // GeometryCollection(_) => self.as_geometry_collection().haversine_length(),
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
    fn haversine_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray = (vec![input_geom].as_slice(), Dimension::XY).into();
        let result_array = input_array.haversine_length();

        // Meters
        let expected = 5_570_230.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
