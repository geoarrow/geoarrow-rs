use crate::array::{LineStringArray, PointArray};
use crate::scalar::Point;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::LineLocatePoint as _LineLocatePoint;

/// Returns a (option of the) fraction of the line's total length
/// representing the location of the closest point on the line to
/// the given point.
///
/// If the line has zero length the fraction returned is zero.
///
/// If either the point's coordinates or any coordinates of the line
/// are not finite, returns `None`.
///
/// # Examples
///
/// ```
/// use geo::{LineString, point};
/// use geoarrow::algorithm::geo::LineLocatePoint;
/// use geoarrow::array::LineStringArray;
/// use arrow_array::array::Array;
///
/// let linestring: LineString = vec![
///     [-1.0, 0.0],
///     [0.0, 0.0],
///     [0.0, 1.0]
/// ].into();
/// let linestring_array: LineStringArray<i32> = vec![linestring].as_slice().into();
///
/// let result = linestring_array.line_locate_point(&point!(x: -1.0, y: 0.0));
/// assert_eq!(result.value(0), 0.0);
/// assert!(result.is_valid(0));
///
/// let result = linestring_array.line_locate_point(&point!(x: -0.5, y: 0.0));
/// assert_eq!(result.value(0), 0.25);
/// assert!(result.is_valid(0));
///
/// let result = linestring_array.line_locate_point(&point!(x: 0.0, y: 0.0));
/// assert_eq!(result.value(0), 0.5);
/// assert!(result.is_valid(0));
/// ```
pub trait LineLocatePoint<Rhs> {
    fn line_locate_point(&self, p: &Rhs) -> Float64Array;
}

impl<O: OffsetSizeTrait> LineLocatePoint<PointArray> for LineStringArray<O> {
    fn line_locate_point(&self, p: &PointArray) -> Float64Array {
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo()
            .zip(p.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => {
                    output_array.append_option(first.line_locate_point(&second))
                }
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

impl<'a, O: OffsetSizeTrait> LineLocatePoint<Point<'a>> for LineStringArray<O> {
    fn line_locate_point(&self, p: &Point<'a>) -> Float64Array {
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_line_string| {
            let output = maybe_line_string
                .and_then(|line_string| line_string.line_locate_point(&p.to_geo()));
            output_array.append_option(output)
        });

        output_array.finish()
    }
}

impl<O: OffsetSizeTrait> LineLocatePoint<geo::Point> for LineStringArray<O> {
    fn line_locate_point(&self, p: &geo::Point) -> Float64Array {
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_line_string| {
            let output = maybe_line_string.and_then(|line_string| line_string.line_locate_point(p));
            output_array.append_option(output)
        });

        output_array.finish()
    }
}
