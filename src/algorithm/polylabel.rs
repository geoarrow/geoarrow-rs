use polylabel::polylabel;

use crate::algorithm::native::UnaryPoint;
use crate::array::{AsChunkedNativeArray, AsNativeArray, PointArray, PolygonArray};
use crate::chunked_array::{
    ChunkedGeometryArray, ChunkedNativeArray, ChunkedPointArray, ChunkedPolygonArray,
};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;

/// Calculate a Polygon's ideal label position by calculating its _pole of inaccessibility_.
///
/// The pole of inaccessibility is the most distant internal point from the polygon outline (not to
/// be confused with centroid), and is useful for optimal placement of a text label on a polygon.
///
/// The calculation uses an iterative grid-based algorithm, ported from the original [JavaScript
/// implementation](https://github.com/mapbox/polylabel).
///
/// This binds to the existing Rust implementation in [mod@polylabel].
pub trait Polylabel {
    type Output;

    fn polylabel(&self, tolerance: f64) -> Self::Output;
}

impl Polylabel for PolygonArray<2> {
    type Output = Result<PointArray<2>>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        Ok(self.try_unary_point(|geom| polylabel(&geom.to_geo(), &tolerance))?)
    }
}

impl Polylabel for &dyn NativeArray {
    type Output = Result<PointArray<2>>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        match self.data_type() {
            NativeType::Polygon(_, Dimension::XY) => self.as_polygon::<2>().polylabel(tolerance),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl Polylabel for ChunkedPolygonArray<2> {
    type Output = Result<ChunkedPointArray<2>>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        let chunks = self.try_map(|chunk| chunk.polylabel(tolerance))?;
        Ok(ChunkedGeometryArray::new(chunks))
    }
}

impl Polylabel for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPointArray<2>>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        match self.data_type() {
            NativeType::Polygon(_, Dimension::XY) => self.as_polygon::<2>().polylabel(tolerance),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
