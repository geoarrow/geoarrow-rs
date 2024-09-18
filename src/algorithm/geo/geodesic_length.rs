use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedGeometryArrayTrait};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::GeodesicLength as _GeodesicLength;

/// Determine the length of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)]. As opposed to older methods
/// like Vincenty, this method is accurate to a few nanometers and always converges.
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
pub trait GeodesicLength {
    type Output;

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
    /// let linestring_array: LineStringArray<i32, 2> = vec![linestring].as_slice().into();
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
    fn geodesic_length(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl GeodesicLength for PointArray<2> {
    type Output = Float64Array;

    fn geodesic_length(&self) -> Self::Output {
        zeroes(self.len(), self.nulls())
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            type Output = Float64Array;

            fn geodesic_length(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(MultiPointArray<O, 2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            type Output = Float64Array;

            fn geodesic_length(&self) -> Self::Output {
                self.unary_primitive(|geom| geom.to_geo().geodesic_length())
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>);

impl GeodesicLength for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn geodesic_length(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().geodesic_length(),
            LineString(_, XY) => self.as_line_string::<2>().geodesic_length(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().geodesic_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().geodesic_length(),
            // LargePolygon(_, XY) => self.as_large_polygon::<2>().geodesic_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().geodesic_length(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().geodesic_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().geodesic_length(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().geodesic_length(),
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().geodesic_length(),
            // LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().geodesic_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().geodesic_length(),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().geodesic_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().geodesic_length(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().geodesic_length()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl GeodesicLength for ChunkedGeometryArray<PointArray<2>> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn geodesic_length(&self) -> Self::Output {
        self.map(|chunk| chunk.geodesic_length()).try_into()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicLength for $type {
            type Output = Result<ChunkedArray<Float64Array>>;

            fn geodesic_length(&self) -> Self::Output {
                self.map(|chunk| chunk.geodesic_length()).try_into()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O, 2>>);

impl GeodesicLength for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn geodesic_length(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().geodesic_length(),
            LineString(_, XY) => self.as_line_string::<2>().geodesic_length(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().geodesic_length(),
            // Polygon(_, XY) => self.as_polygon::<2>().geodesic_length(),
            // LargePolygon(_, XY) => self.as_large_polygon::<2>().geodesic_length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().geodesic_length(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().geodesic_length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().geodesic_length(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().geodesic_length(),
            // MultiPolygon(_, XY) => self.as_multi_polygon::<2>().geodesic_length(),
            // LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().geodesic_length(),
            // Mixed(_, XY) => self.as_mixed::<2>().geodesic_length(),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().geodesic_length(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().geodesic_length(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().geodesic_length()
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
    fn geodesic_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
            // Osaka
            (x: 135.5244559, y: 34.687455),
        ];
        let input_array: LineStringArray<i64, 2> = vec![input_geom].as_slice().into();
        let result_array = input_array.geodesic_length();

        // Meters
        let expected = 15_109_158.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
