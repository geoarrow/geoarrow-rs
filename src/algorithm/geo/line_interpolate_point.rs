use crate::array::LineStringArray;
use crate::array::*;
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
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
    fn line_interpolate_point(&self, fraction: &Rhs) -> PointArray;
}

impl<O: OffsetSizeTrait> LineInterpolatePoint<Float64Array> for LineStringArray<O> {
    fn line_interpolate_point(&self, p: &Float64Array) -> PointArray {
        let mut output_array = PointBuilder::with_capacity(self.len());

        self.iter_geo()
            .zip(p)
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(fraction)) => {
                    output_array.push_point(first.line_interpolate_point(fraction).as_ref())
                }
                _ => output_array.push_null(),
            });

        output_array.into()
    }
}

impl<O: OffsetSizeTrait> LineInterpolatePoint<f64> for LineStringArray<O> {
    fn line_interpolate_point(&self, p: &f64) -> PointArray {
        let mut output_array = PointBuilder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_line_string| {
            let output =
                maybe_line_string.and_then(|line_string| line_string.line_interpolate_point(*p));
            output_array.push_point(output.as_ref())
        });

        output_array.into()
    }
}
