use crate::algorithm::native::{Binary, MapChunks, Unary};
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArrayTrait, ChunkedLineStringArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geo::line_string_to_geo;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::FrechetDistance as _FrechetDistance;

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

/// Determine the similarity between two arrays of `LineStrings` using the [Frechet distance].
///
/// Based on [Computing Discrete Frechet Distance] by T. Eiter and H. Mannila.
///
/// [Frechet distance]: https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance
/// [Computing Discrete Frechet Distance]: http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
pub trait FrechetDistance<Rhs = Self> {
    type Output;

    fn frechet_distance(&self, rhs: &Rhs) -> Self::Output;
}

impl<O1: OffsetSizeTrait, O2: OffsetSizeTrait> FrechetDistance<LineStringArray<O2, 2>>
    for LineStringArray<O1, 2>
{
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &LineStringArray<O2, 2>) -> Self::Output {
        self.try_binary_primitive(rhs, |left, right| {
            Ok(left.to_geo().frechet_distance(&right.to_geo()))
        })
        .unwrap()
    }
}

impl<O1: OffsetSizeTrait, O2: OffsetSizeTrait> FrechetDistance<ChunkedLineStringArray<O2, 2>>
    for ChunkedLineStringArray<O1, 2>
{
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &ChunkedLineStringArray<O2, 2>) -> Self::Output {
        ChunkedArray::new(self.binary_map(rhs.chunks(), |(left, right)| {
            FrechetDistance::frechet_distance(left, right)
        }))
    }
}

impl FrechetDistance for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        let result = match (self.data_type(), rhs.data_type()) {
            (
                GeoDataType::LineString(_, Dimension::XY),
                GeoDataType::LineString(_, Dimension::XY),
            ) => {
                FrechetDistance::frechet_distance(self.as_line_string::<2>(), rhs.as_line_string::<2>())
            }
            (
                GeoDataType::LineString(_, Dimension::XY),
                GeoDataType::LargeLineString(_, Dimension::XY),
            ) => FrechetDistance::frechet_distance(
                self.as_line_string::<2>(),
                rhs.as_large_line_string::<2>(),
            ),
            (
                GeoDataType::LargeLineString(_, Dimension::XY),
                GeoDataType::LineString(_, Dimension::XY),
            ) => FrechetDistance::frechet_distance(
                self.as_large_line_string::<2>(),
                rhs.as_line_string::<2>(),
            ),
            (
                GeoDataType::LargeLineString(_, Dimension::XY),
                GeoDataType::LargeLineString(_, Dimension::XY),
            ) => FrechetDistance::frechet_distance(
                self.as_large_line_string::<2>(),
                rhs.as_large_line_string::<2>(),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl FrechetDistance for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        let result = match (self.data_type(), rhs.data_type()) {
            (
                GeoDataType::LineString(_, Dimension::XY),
                GeoDataType::LineString(_, Dimension::XY),
            ) => {
                FrechetDistance::frechet_distance(self.as_line_string::<2>(), rhs.as_line_string::<2>())
            }
            (
                GeoDataType::LineString(_, Dimension::XY),
                GeoDataType::LargeLineString(_, Dimension::XY),
            ) => FrechetDistance::frechet_distance(
                self.as_line_string::<2>(),
                rhs.as_large_line_string::<2>(),
            ),
            (
                GeoDataType::LargeLineString(_, Dimension::XY),
                GeoDataType::LineString(_, Dimension::XY),
            ) => FrechetDistance::frechet_distance(
                self.as_large_line_string::<2>(),
                rhs.as_line_string::<2>(),
            ),
            (
                GeoDataType::LargeLineString(_, Dimension::XY),
                GeoDataType::LargeLineString(_, Dimension::XY),
            ) => FrechetDistance::frechet_distance(
                self.as_large_line_string::<2>(),
                rhs.as_large_line_string::<2>(),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

pub trait FrechetDistanceLineString<Rhs> {
    type Output;

    fn frechet_distance(&self, rhs: &Rhs) -> Self::Output;
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> FrechetDistanceLineString<G>
    for LineStringArray<O, 2>
{
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        self.try_unary_primitive(|geom| {
            Ok::<_, GeoArrowError>(geom.to_geo().frechet_distance(&rhs))
        })
        .unwrap()
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64> + Sync> FrechetDistanceLineString<G>
    for ChunkedLineStringArray<O, 2>
{
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        ChunkedArray::new(self.map(|chunk| FrechetDistanceLineString::frechet_distance(chunk, rhs)))
    }
}

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::LineString(_, Dimension::XY) => {
                FrechetDistanceLineString::frechet_distance(self.as_line_string::<2>(), rhs)
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                FrechetDistanceLineString::frechet_distance(self.as_large_line_string::<2>(), rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        let result = match self.data_type() {
            GeoDataType::LineString(_, Dimension::XY) => {
                FrechetDistanceLineString::frechet_distance(self.as_line_string::<2>(), &rhs)
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                FrechetDistanceLineString::frechet_distance(self.as_large_line_string::<2>(), &rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
