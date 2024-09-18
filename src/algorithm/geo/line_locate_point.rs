use crate::algorithm::native::MapChunks;
use crate::array::{AsChunkedNativeArray, AsNativeArray, LineStringArray, PointArray};
use crate::chunked_array::{ChunkedArray, ChunkedLineStringArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::PointTrait;
use crate::io::geo::point_to_geo;
use crate::trait_::NativeArrayAccessor;
use crate::NativeArray;
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
pub trait LineLocatePoint<Rhs> {
    type Output;

    fn line_locate_point(&self, rhs: Rhs) -> Self::Output;
}

impl<O: OffsetSizeTrait> LineLocatePoint<&PointArray<2>> for LineStringArray<O, 2> {
    type Output = Float64Array;

    fn line_locate_point(&self, rhs: &PointArray<2>) -> Float64Array {
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo()
            .zip(rhs.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => {
                    if let Some(val) = first.line_locate_point(&second) {
                        output_array.append_value(val)
                    } else {
                        output_array.append_value(f64::NAN)
                    }
                }
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

impl LineLocatePoint<&dyn NativeArray> for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn line_locate_point(&self, rhs: &dyn NativeArray) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match (self.data_type(), rhs.data_type()) {
            (LineString(_, XY), Point(_, XY)) => {
                LineLocatePoint::line_locate_point(self.as_line_string::<2>(), rhs.as_point::<2>())
            }
            (LargeLineString(_, XY), Point(_, XY)) => LineLocatePoint::line_locate_point(
                self.as_large_line_string::<2>(),
                rhs.as_point::<2>(),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<O: OffsetSizeTrait> LineLocatePoint<&[PointArray<2>]> for ChunkedLineStringArray<O, 2> {
    type Output = ChunkedArray<Float64Array>;

    fn line_locate_point(&self, rhs: &[PointArray<2>]) -> ChunkedArray<Float64Array> {
        let chunks = self.binary_map(rhs, |(left, right)| {
            LineLocatePoint::line_locate_point(left, right)
        });
        ChunkedArray::new(chunks)
    }
}

impl LineLocatePoint<&dyn ChunkedNativeArray> for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn line_locate_point(&self, rhs: &dyn ChunkedNativeArray) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match (self.data_type(), rhs.data_type()) {
            (LineString(_, XY), Point(_, XY)) => LineLocatePoint::line_locate_point(
                self.as_line_string::<2>(),
                &rhs.as_point::<2>().chunks,
            ),
            (LargeLineString(_, XY), Point(_, XY)) => LineLocatePoint::line_locate_point(
                self.as_large_line_string::<2>(),
                &rhs.as_point::<2>().chunks,
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

pub trait LineLocatePointScalar<Rhs> {
    type Output;

    fn line_locate_point(&self, rhs: Rhs) -> Self::Output;
}

impl<O: OffsetSizeTrait, G: PointTrait<T = f64>> LineLocatePointScalar<G>
    for LineStringArray<O, 2>
{
    type Output = Float64Array;

    fn line_locate_point(&self, rhs: G) -> Self::Output {
        let rhs = point_to_geo(&rhs);

        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_line_string| {
            if let Some(line_string) = maybe_line_string {
                if let Some(val) = line_string.line_locate_point(&rhs) {
                    output_array.append_value(val)
                } else {
                    output_array.append_value(f64::NAN)
                }
            }
        });

        output_array.finish()
    }
}

impl<G: PointTrait<T = f64>> LineLocatePointScalar<G> for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn line_locate_point(&self, rhs: G) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            LineString(_, XY) => {
                LineLocatePointScalar::line_locate_point(self.as_line_string::<2>(), rhs)
            }
            LargeLineString(_, XY) => {
                LineLocatePointScalar::line_locate_point(self.as_large_line_string::<2>(), rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<O: OffsetSizeTrait, G: PointTrait<T = f64>> LineLocatePointScalar<G>
    for ChunkedLineStringArray<O, 2>
{
    type Output = ChunkedArray<Float64Array>;

    fn line_locate_point(&self, rhs: G) -> Self::Output {
        let rhs = point_to_geo(&rhs);
        let chunks = self.map(|chunk| LineLocatePointScalar::line_locate_point(chunk, rhs));
        ChunkedArray::new(chunks)
    }
}

impl<G: PointTrait<T = f64>> LineLocatePointScalar<G> for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn line_locate_point(&self, rhs: G) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            LineString(_, XY) => {
                LineLocatePointScalar::line_locate_point(self.as_line_string::<2>(), rhs)
            }
            LargeLineString(_, XY) => {
                LineLocatePointScalar::line_locate_point(self.as_large_line_string::<2>(), rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
