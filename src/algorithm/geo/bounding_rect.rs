use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use arrow2::types::Offset;
use geo::algorithm::bounding_rect::BoundingRect as GeoBoundingRect;
use geo::Polygon;

/// Calculation of the bounding rectangle of a geometry.
pub trait BoundingRect<O: Offset> {
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
    fn bounding_rect(&self) -> PolygonArray<O>;
}

impl<C: CoordBuffer, O: Offset> BoundingRect<O> for PointArray {
    fn bounding_rect(&self) -> PolygonArray<O> {
        let output_geoms: Vec<Option<Polygon>> = self
            .iter_geo()
            .map(|maybe_g| maybe_g.map(|geom| geom.bounding_rect().to_polygon()))
            .collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<C: CoordBuffer, O: Offset> BoundingRect<O> for $type {
            fn bounding_rect(&self) -> PolygonArray<O> {
                let output_geoms: Vec<Option<Polygon>> = self
                    .iter_geo()
                    .map(|maybe_g| {
                        maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                    })
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
iter_geo_impl!(WKBArray<O>);

impl<C: CoordBuffer, O: Offset> BoundingRect<O> for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn bounding_rect(&self) -> PolygonArray<O>;
    }
}
