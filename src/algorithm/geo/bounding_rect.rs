use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedGeometryArrayTrait};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::algorithm::bounding_rect::BoundingRect as GeoBoundingRect;
use geo::Rect;

/// Calculation of the bounding rectangle of a geometry.
pub trait BoundingRect {
    type Output;

    /// Return the bounding rectangle of a geometry
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::BoundingRect;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 116.34),
    ///     (x: 42.02f64, y: 118.34),
    /// ];
    ///
    /// let bounding_rect = line_string.bounding_rect().unwrap();
    ///
    /// assert_eq!(40.02f64, bounding_rect.min().x);
    /// assert_eq!(42.02f64, bounding_rect.max().x);
    /// assert_eq!(116.34, bounding_rect.min().y);
    /// assert_eq!(118.34, bounding_rect.max().y);
    /// ```
    fn bounding_rect(&self) -> Self::Output;
}

impl BoundingRect for PointArray<2> {
    type Output = RectArray<2>;

    fn bounding_rect(&self) -> Self::Output {
        let output_geoms: Vec<Option<Rect>> = self
            .iter_geo()
            .map(|maybe_g| maybe_g.map(|geom| geom.bounding_rect()))
            .collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> BoundingRect for $type {
            type Output = RectArray<2>;

            fn bounding_rect(&self) -> Self::Output {
                let output_geoms: Vec<Option<Rect>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.and_then(|geom| geom.bounding_rect()))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>);
iter_geo_impl!(MixedGeometryArray<O, 2>);
iter_geo_impl!(GeometryCollectionArray<O, 2>);
iter_geo_impl!(WKBArray<O>);

impl BoundingRect for &dyn GeometryArrayTrait {
    type Output = Result<RectArray<2>>;

    fn bounding_rect(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point::<2>().bounding_rect(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string::<2>().bounding_rect(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string::<2>().bounding_rect()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon::<2>().bounding_rect(),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                self.as_large_polygon::<2>().bounding_rect()
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point::<2>().bounding_rect(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point::<2>().bounding_rect()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string::<2>().bounding_rect()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string::<2>().bounding_rect()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                self.as_multi_polygon::<2>().bounding_rect()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon::<2>().bounding_rect()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed::<2>().bounding_rect(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed::<2>().bounding_rect(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection::<2>().bounding_rect()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection::<2>().bounding_rect()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> BoundingRect for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<RectArray<2>>>;

    fn bounding_rect(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().bounding_rect())?
            .try_into()
    }
}

impl BoundingRect for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedGeometryArray<RectArray<2>>>;

    fn bounding_rect(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point::<2>().bounding_rect(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string::<2>().bounding_rect(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string::<2>().bounding_rect()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon::<2>().bounding_rect(),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                self.as_large_polygon::<2>().bounding_rect()
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point::<2>().bounding_rect(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point::<2>().bounding_rect()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string::<2>().bounding_rect()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string::<2>().bounding_rect()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                self.as_multi_polygon::<2>().bounding_rect()
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon::<2>().bounding_rect()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed::<2>().bounding_rect(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed::<2>().bounding_rect(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection::<2>().bounding_rect()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection::<2>().bounding_rect()
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
