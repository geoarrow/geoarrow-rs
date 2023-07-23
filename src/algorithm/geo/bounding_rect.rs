use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, RectArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow2::buffer::Buffer;
use arrow2::types::Offset;
use geo::algorithm::bounding_rect::BoundingRect as GeoBoundingRect;
use geo::Polygon;

/// Calculation of the bounding rectangle of a geometry.
pub trait BoundingRect {
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
    fn bounding_rect(&self) -> RectArray;
}

impl BoundingRect for PointArray {
    fn bounding_rect(&self) -> RectArray {
        let mut values: Vec<f64> = Vec::with_capacity(self.len() * 4);

        self.iter_geo()
            .for_each(|maybe_g| match maybe_g.map(|geom| geom.bounding_rect()) {
                Some(bounds) => {
                    values.push(bounds.min().x);
                    values.push(bounds.min().y);
                    values.push(bounds.max().x);
                    values.push(bounds.max().y);
                }
                // For a fixed size array, we have to allocate null regions
                None => {
                    values.push(0.0f64);
                    values.push(0.0f64);
                    values.push(0.0f64);
                    values.push(0.0f64);
                }
            });

        let validity = self.validity().cloned();

        RectArray::new(values.into(), validity)
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: Offset> BoundingRect for $type {
            fn bounding_rect(&self) -> RectArray {
        let mut values: Vec<f64> = Vec::with_capacity(self.len() * 4);

        self.iter_geo()
            .for_each(|maybe_g| match maybe_g.map(|geom| geom.bounding_rect()) {
                Some(bounds) => {
                    values.push(bounds.min().x);
                    values.push(bounds.min().y);
                    values.push(bounds.max().x);
                    values.push(bounds.max().y);
                }
                // For a fixed size array, we have to allocate null regions
                None => {
                    values.push(0.0f64);
                    values.push(0.0f64);
                    values.push(0.0f64);
                    values.push(0.0f64);
                }
            });

        let validity = self.validity().cloned();

        RectArray::new(values.into(), validity)

        // let output_geoms: Vec<Option<Polygon>> = self
        //             .iter_geo()
        //             .map(|maybe_g| {
        //                 maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
        //             })
        //             .collect();

        //         output_geoms.into()
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

impl<O: Offset> BoundingRect for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn bounding_rect(&self) -> RectArray;
    }
}
