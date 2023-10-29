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
/// use geo::LineLocatePoint;
///
/// let linestring: LineString = vec![
///     [-1.0, 0.0],
///     [0.0, 0.0],
///     [0.0, 1.0]
/// ].into();
///
/// assert_eq!(linestring.line_locate_point(&point!(x: -1.0, y: 0.0)), Some(0.0));
/// assert_eq!(linestring.line_locate_point(&point!(x: -0.5, y: 0.0)), Some(0.25));
/// assert_eq!(linestring.line_locate_point(&point!(x: 0.0, y: 0.0)), Some(0.5));
/// assert_eq!(linestring.line_locate_point(&point!(x: 0.0, y: 0.5)), Some(0.75));
/// assert_eq!(linestring.line_locate_point(&point!(x: 0.0, y: 1.0)), Some(1.0));
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
