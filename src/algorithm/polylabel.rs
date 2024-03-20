use arrow_array::OffsetSizeTrait;
use polylabel::polylabel;

use crate::algorithm::native::Unary;
use crate::array::{AsChunkedGeometryArray, AsGeometryArray, PointArray, PolygonArray};
use crate::chunked_array::{
    ChunkedGeometryArray, ChunkedGeometryArrayTrait, ChunkedPointArray, ChunkedPolygonArray,
};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;

/// Calculate a Polygon's ideal label position by calculating its _pole of inaccessibility_.
///
/// The pole of inaccessibility is the most distant internal point from the polygon outline (not to
/// be confused with centroid), and is useful for optimal placement of a text label on a polygon.
///
/// The calculation uses an iterative grid-based algorithm, ported from the original [JavaScript
/// implementation](https://github.com/mapbox/polylabel).
///
/// This binds to the existing Rust implementation in [polylabel].
pub trait Polylabel {
    type Output;

    fn polylabel(&self, tolerance: f64) -> Self::Output;
}

impl<O: OffsetSizeTrait> Polylabel for PolygonArray<O> {
    type Output = Result<PointArray>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        Ok(self.try_unary_point(|geom| polylabel(&geom.to_geo(), &tolerance))?)
    }
}

impl Polylabel for &dyn GeometryArrayTrait {
    type Output = Result<PointArray>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        match self.data_type() {
            GeoDataType::Polygon(_) => self.as_polygon().polylabel(tolerance),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().polylabel(tolerance),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<O: OffsetSizeTrait> Polylabel for ChunkedPolygonArray<O> {
    type Output = Result<ChunkedPointArray>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        let chunks = self.try_map(|chunk| chunk.polylabel(tolerance))?;
        Ok(ChunkedGeometryArray::new(chunks))
    }
}

impl Polylabel for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedPointArray>;

    fn polylabel(&self, tolerance: f64) -> Self::Output {
        match self.data_type() {
            GeoDataType::Polygon(_) => self.as_polygon().polylabel(tolerance),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().polylabel(tolerance),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
