use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::{Float64Array, OffsetSizeTrait};
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
    fn vincenty_length(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl VincentyLength for PointArray {
    type Output = Result<Float64Array>;

    fn vincenty_length(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> VincentyLength for $type {
            type Output = Result<Float64Array>;

            fn vincenty_length(&self) -> Self::Output {
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
            type Output = Result<Float64Array>;

            fn vincenty_length(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geo().vincenty_length())?)
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);

impl VincentyLength for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn vincenty_length(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().vincenty_length(),
            GeoDataType::LineString(_) => self.as_line_string().vincenty_length(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().vincenty_length(),
            // GeoDataType::Polygon(_) => self.as_polygon().vincenty_length(),
            // GeoDataType::LargePolygon(_) => self.as_large_polygon().vincenty_length(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().vincenty_length(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().vincenty_length(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().vincenty_length(),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().vincenty_length()
            }
            // GeoDataType::MultiPolygon(_) => self.as_multi_polygon().vincenty_length(),
            // GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().vincenty_length(),
            // GeoDataType::Mixed(_) => self.as_mixed().vincenty_length(),
            // GeoDataType::LargeMixed(_) => self.as_large_mixed().vincenty_length(),
            // GeoDataType::GeometryCollection(_) => self.as_geometry_collection().vincenty_length(),
            // GeoDataType::LargeGeometryCollection(_) => {
            //     self.as_large_geometry_collection().vincenty_length()
            // }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl VincentyLength for ChunkedGeometryArray<PointArray> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn vincenty_length(&self) -> Self::Output {
        self.try_map(|chunk| chunk.vincenty_length())?.try_into()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> VincentyLength for $type {
            type Output = Result<ChunkedArray<Float64Array>>;

            fn vincenty_length(&self) -> Self::Output {
                self.try_map(|chunk| chunk.vincenty_length())?.try_into()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O>>);

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
