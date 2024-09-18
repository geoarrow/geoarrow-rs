use crate::algorithm::native::MapChunks;
use crate::array::LineStringArray;
use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArrayTrait, ChunkedLineStringArray, ChunkedPointArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::NativeArray;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::LineInterpolatePoint as _LineInterpolatePoint;

/// Returns an option of the point that lies a given fraction along the line.
///
/// If the given fraction is
///  * less than zero (including negative infinity): returns a `Some`
///    of the starting point
///  * greater than one (including infinity): returns a `Some` of the ending point
///
///  If either the fraction is NaN, or any coordinates of the line are not
///  finite, returns `None`.
///
/// # Examples
///
/// ```
/// use geo::{LineString, point};
/// use geo::LineInterpolatePoint;
///
/// let linestring: LineString = vec![
///     [-1.0, 0.0],
///     [0.0, 0.0],
///     [0.0, 1.0]
/// ].into();
///
/// assert_eq!(linestring.line_interpolate_point(-1.0), Some(point!(x: -1.0, y: 0.0)));
/// assert_eq!(linestring.line_interpolate_point(0.25), Some(point!(x: -0.5, y: 0.0)));
/// assert_eq!(linestring.line_interpolate_point(0.5), Some(point!(x: 0.0, y: 0.0)));
/// assert_eq!(linestring.line_interpolate_point(0.75), Some(point!(x: 0.0, y: 0.5)));
/// assert_eq!(linestring.line_interpolate_point(2.0), Some(point!(x: 0.0, y: 1.0)));
/// ```
pub trait LineInterpolatePoint<Rhs> {
    type Output;

    fn line_interpolate_point(&self, fraction: Rhs) -> Self::Output;
}

impl<O: OffsetSizeTrait> LineInterpolatePoint<&Float64Array> for LineStringArray<O, 2> {
    type Output = PointArray<2>;

    fn line_interpolate_point(&self, p: &Float64Array) -> Self::Output {
        let mut output_array = PointBuilder::with_capacity(self.len());

        self.iter_geo()
            .zip(p)
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(fraction)) => {
                    if let Some(val) = first.line_interpolate_point(fraction) {
                        output_array.push_point(Some(&val))
                    } else {
                        output_array.push_empty()
                    }
                }
                _ => output_array.push_null(),
            });

        output_array.into()
    }
}

impl LineInterpolatePoint<&Float64Array> for &dyn NativeArray {
    type Output = Result<PointArray<2>>;

    fn line_interpolate_point(&self, fraction: &Float64Array) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            LineString(_, XY) => Ok(self.as_line_string::<2>().line_interpolate_point(fraction)),
            LargeLineString(_, XY) => Ok(self
                .as_large_line_string::<2>()
                .line_interpolate_point(fraction)),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<O: OffsetSizeTrait> LineInterpolatePoint<&[Float64Array]> for ChunkedLineStringArray<O, 2> {
    type Output = ChunkedPointArray<2>;

    fn line_interpolate_point(&self, p: &[Float64Array]) -> Self::Output {
        ChunkedPointArray::new(
            self.binary_map(p, |(left, right)| left.line_interpolate_point(right)),
        )
    }
}

impl LineInterpolatePoint<&[Float64Array]> for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedPointArray<2>>;

    fn line_interpolate_point(&self, fraction: &[Float64Array]) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            LineString(_, XY) => Ok(self.as_line_string::<2>().line_interpolate_point(fraction)),
            LargeLineString(_, XY) => Ok(self
                .as_large_line_string::<2>()
                .line_interpolate_point(fraction)),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<O: OffsetSizeTrait> LineInterpolatePoint<f64> for LineStringArray<O, 2> {
    type Output = PointArray<2>;

    fn line_interpolate_point(&self, p: f64) -> Self::Output {
        let mut output_array = PointBuilder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_line_string| {
            if let Some(line_string) = maybe_line_string {
                if let Some(val) = line_string.line_interpolate_point(p) {
                    output_array.push_point(Some(&val))
                } else {
                    output_array.push_empty()
                }
            } else {
                output_array.push_null()
            }
        });

        output_array.into()
    }
}

impl LineInterpolatePoint<f64> for &dyn NativeArray {
    type Output = Result<PointArray<2>>;

    fn line_interpolate_point(&self, fraction: f64) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            LineString(_, XY) => Ok(self.as_line_string::<2>().line_interpolate_point(fraction)),
            LargeLineString(_, XY) => Ok(self
                .as_large_line_string::<2>()
                .line_interpolate_point(fraction)),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<O: OffsetSizeTrait> LineInterpolatePoint<f64> for ChunkedLineStringArray<O, 2> {
    type Output = ChunkedPointArray<2>;

    fn line_interpolate_point(&self, fraction: f64) -> Self::Output {
        ChunkedPointArray::new(self.map(|chunk| chunk.line_interpolate_point(fraction)))
    }
}

impl LineInterpolatePoint<f64> for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedPointArray<2>>;

    fn line_interpolate_point(&self, fraction: f64) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            LineString(_, XY) => Ok(self.as_line_string::<2>().line_interpolate_point(fraction)),
            LargeLineString(_, XY) => Ok(self
                .as_large_line_string::<2>()
                .line_interpolate_point(fraction)),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
