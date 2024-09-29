use crate::algorithm::native::{Binary, MapChunks, Unary};
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedLineStringArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geo::line_string_to_geo;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::Float64Array;
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

impl FrechetDistance<LineStringArray<2>> for LineStringArray<2> {
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &LineStringArray<2>) -> Self::Output {
        self.try_binary_primitive(rhs, |left, right| Ok(left.to_geo().frechet_distance(&right.to_geo()))).unwrap()
    }
}

impl FrechetDistance<ChunkedLineStringArray<2>> for ChunkedLineStringArray<2> {
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &ChunkedLineStringArray<2>) -> Self::Output {
        ChunkedArray::new(self.binary_map(rhs.chunks(), |(left, right)| FrechetDistance::frechet_distance(left, right)))
    }
}

impl FrechetDistance for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match (self.data_type(), rhs.data_type()) {
            (LineString(_, XY), LineString(_, XY)) => FrechetDistance::frechet_distance(self.as_line_string::<2>(), rhs.as_line_string::<2>()),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl FrechetDistance for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &Self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match (self.data_type(), rhs.data_type()) {
            (LineString(_, XY), LineString(_, XY)) => FrechetDistance::frechet_distance(self.as_line_string::<2>(), rhs.as_line_string::<2>()),
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

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for LineStringArray<2> {
    type Output = Float64Array;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        self.try_unary_primitive(|geom| Ok::<_, GeoArrowError>(geom.to_geo().frechet_distance(&rhs))).unwrap()
    }
}

impl<G: LineStringTrait<T = f64> + Sync> FrechetDistanceLineString<G> for ChunkedLineStringArray<2> {
    type Output = ChunkedArray<Float64Array>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        ChunkedArray::new(self.map(|chunk| FrechetDistanceLineString::frechet_distance(chunk, rhs)))
    }
}

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            LineString(_, XY) => FrechetDistanceLineString::frechet_distance(self.as_line_string::<2>(), rhs),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: LineStringTrait<T = f64>> FrechetDistanceLineString<G> for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn frechet_distance(&self, rhs: &G) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let rhs = line_string_to_geo(rhs);
        let result = match self.data_type() {
            LineString(_, XY) => FrechetDistanceLineString::frechet_distance(self.as_line_string::<2>(), &rhs),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
