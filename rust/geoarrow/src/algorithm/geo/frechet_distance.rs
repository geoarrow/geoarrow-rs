use crate::NativeArray;
use crate::algorithm::native::{Binary, MapChunks, Unary};
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedLineStringArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use arrow_array::Float64Array;
use geo::Euclidean;
use geo::line_measures::FrechetDistance as _FrechetDistance;
// use geo::FrechetDistance as _FrechetDistance;
use geo_traits::LineStringTrait;
use geo_traits::to_geo::ToGeoLineString;

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

impl FrechetDistance<LineStringArray> for LineStringArray {
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &LineStringArray) -> Self::Output {
        self.try_binary_primitive(rhs, |left, right| {
            Ok(Euclidean.frechet_distance(&left.to_geo(), &right.to_geo()))
        })
        .unwrap()
    }
}

impl FrechetDistance<ChunkedLineStringArray> for ChunkedLineStringArray {
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &ChunkedLineStringArray) -> Self::Output {
        ChunkedArray::new(self.binary_map(rhs.chunks(), |(left, right)| {
            FrechetDistance::frechet_distance(left, right)
        }))
    }
}

impl FrechetDistance for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        use NativeType::*;

        let result = match (self.data_type(), rhs.data_type()) {
            (LineString(_), LineString(_)) => {
                FrechetDistance::frechet_distance(self.as_line_string(), rhs.as_line_string())
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl FrechetDistance for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        use NativeType::*;

        let result = match (self.data_type(), rhs.data_type()) {
            (LineString(_), LineString(_)) => {
                FrechetDistance::frechet_distance(self.as_line_string(), rhs.as_line_string())
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

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for LineStringArray {
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_line_string();
        self.try_unary_primitive(|geom| {
            Ok::<_, GeoArrowError>(Euclidean.frechet_distance(&geom.to_geo(), &rhs))
        })
        .unwrap()
    }
}

impl<G: LineStringTrait<T = f64> + Sync> FrechetDistanceLineString<G> for ChunkedLineStringArray {
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        ChunkedArray::new(self.map(|chunk| FrechetDistanceLineString::frechet_distance(chunk, rhs)))
    }
}

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            LineString(_) => {
                FrechetDistanceLineString::frechet_distance(self.as_line_string(), rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        use NativeType::*;

        let rhs = rhs.to_line_string();
        let result = match self.data_type() {
            LineString(_) => {
                FrechetDistanceLineString::frechet_distance(self.as_line_string(), &rhs)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
