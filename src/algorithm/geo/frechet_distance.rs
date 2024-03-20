use crate::algorithm::native::{Binary, MapChunks, Unary};
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArrayTrait, ChunkedLineStringArray};
use crate::datatypes::GeoDataType;
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

impl<O1: OffsetSizeTrait, O2: OffsetSizeTrait> FrechetDistance<LineStringArray<O2>>
    for LineStringArray<O1>
{
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &LineStringArray<O2>) -> Self::Output {
        self.try_binary_primitive(rhs, |left, right| {
            Ok(left.to_geo().frechet_distance(&right.to_geo()))
        })
        .unwrap()
    }
}

impl<O1: OffsetSizeTrait, O2: OffsetSizeTrait> FrechetDistance<ChunkedLineStringArray<O2>>
    for ChunkedLineStringArray<O1>
{
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &ChunkedLineStringArray<O2>) -> Self::Output {
        ChunkedArray::new(self.binary_map(rhs.chunks(), |(left, right)| {
            FrechetDistance::frechet_distance(left, right)
        }))
    }
}

impl FrechetDistance for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        let result = match (self.data_type(), rhs.data_type()) {
            (GeoDataType::LineString(_), GeoDataType::LineString(_)) => {
                FrechetDistance::frechet_distance(self.as_line_string(), rhs.as_line_string())
            }
            (GeoDataType::LineString(_), GeoDataType::LargeLineString(_)) => {
                FrechetDistance::frechet_distance(self.as_line_string(), rhs.as_large_line_string())
            }
            (GeoDataType::LargeLineString(_), GeoDataType::LineString(_)) => {
                FrechetDistance::frechet_distance(self.as_large_line_string(), rhs.as_line_string())
            }
            (GeoDataType::LargeLineString(_), GeoDataType::LargeLineString(_)) => {
                FrechetDistance::frechet_distance(
                    self.as_large_line_string(),
                    rhs.as_large_line_string(),
                )
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl FrechetDistance for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        let result = match (self.data_type(), rhs.data_type()) {
            (GeoDataType::LineString(_), GeoDataType::LineString(_)) => {
                FrechetDistance::frechet_distance(self.as_line_string(), rhs.as_line_string())
            }
            (GeoDataType::LineString(_), GeoDataType::LargeLineString(_)) => {
                FrechetDistance::frechet_distance(self.as_line_string(), rhs.as_large_line_string())
            }
            (GeoDataType::LargeLineString(_), GeoDataType::LineString(_)) => {
                FrechetDistance::frechet_distance(self.as_large_line_string(), rhs.as_line_string())
            }
            (GeoDataType::LargeLineString(_), GeoDataType::LargeLineString(_)) => {
                FrechetDistance::frechet_distance(
                    self.as_large_line_string(),
                    rhs.as_large_line_string(),
                )
            }
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
    for LineStringArray<O>
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
    for ChunkedLineStringArray<O>
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
            GeoDataType::LineString(_) => {
                FrechetDistanceLineString::frechet_distance(self.as_line_string(), rhs)
            }
            GeoDataType::LargeLineString(_) => {
                FrechetDistanceLineString::frechet_distance(self.as_large_line_string(), rhs)
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
            GeoDataType::LineString(_) => {
                FrechetDistanceLineString::frechet_distance(self.as_line_string(), &rhs)
            }
            GeoDataType::LargeLineString(_) => {
                FrechetDistanceLineString::frechet_distance(self.as_large_line_string(), &rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
