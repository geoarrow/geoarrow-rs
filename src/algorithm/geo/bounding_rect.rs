use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
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
        let output_geoms: Vec<Option<Rect>> = self.iter_geo().map(|maybe_g| maybe_g.map(|geom| geom.bounding_rect())).collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl BoundingRect for $type {
            type Output = RectArray<2>;

            fn bounding_rect(&self) -> Self::Output {
                let output_geoms: Vec<Option<Rect>> = self.iter_geo().map(|maybe_g| maybe_g.and_then(|geom| geom.bounding_rect())).collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);

impl BoundingRect for &dyn NativeArray {
    type Output = Result<RectArray<2>>;

    fn bounding_rect(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().bounding_rect(),
            LineString(_, XY) => self.as_line_string::<2>().bounding_rect(),
            Polygon(_, XY) => self.as_polygon::<2>().bounding_rect(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().bounding_rect(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().bounding_rect(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().bounding_rect(),
            Mixed(_, XY) => self.as_mixed::<2>().bounding_rect(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().bounding_rect(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> BoundingRect for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<RectArray<2>>>;

    fn bounding_rect(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().bounding_rect())?.try_into()
    }
}

impl BoundingRect for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedGeometryArray<RectArray<2>>>;

    fn bounding_rect(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().bounding_rect(),
            LineString(_, XY) => self.as_line_string::<2>().bounding_rect(),
            Polygon(_, XY) => self.as_polygon::<2>().bounding_rect(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().bounding_rect(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().bounding_rect(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().bounding_rect(),
            Mixed(_, XY) => self.as_mixed::<2>().bounding_rect(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().bounding_rect(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
