use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
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

impl BoundingRect for PointArray {
    type Output = RectArray;

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
            type Output = RectArray;

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

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(MixedGeometryArray<O>);
iter_geo_impl!(GeometryCollectionArray<O>);
iter_geo_impl!(WKBArray<O>);

impl BoundingRect for &dyn GeometryArrayTrait {
    type Output = Result<RectArray>;

    fn bounding_rect(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().bounding_rect(),
            GeoDataType::LineString(_) => self.as_line_string().bounding_rect(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().bounding_rect(),
            GeoDataType::Polygon(_) => self.as_polygon().bounding_rect(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().bounding_rect(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().bounding_rect(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().bounding_rect(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().bounding_rect(),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().bounding_rect()
            }
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().bounding_rect(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().bounding_rect(),
            GeoDataType::Mixed(_) => self.as_mixed().bounding_rect(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().bounding_rect(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().bounding_rect(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().bounding_rect()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> BoundingRect for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<RectArray>>;

    fn bounding_rect(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().bounding_rect())?
            .try_into()
    }
}
