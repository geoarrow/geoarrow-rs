use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::LineStringArray;
use crate::array::*;
use geo::Scale as _Scale;

/// An affine transformation which scales a geometry up or down by a factor.
///
/// ## Performance
///
/// If you will be performing multiple transformations, like [`Scale`](crate::Scale),
/// [`Skew`](crate::Skew), [`Translate`](crate::Translate), or [`Rotate`](crate::Rotate), it is more
/// efficient to compose the transformations and apply them as a single operation using the
/// [`AffineOps`](crate::AffineOps) trait.
pub trait Scale {
    /// Scale a geometry from it's bounding box center.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Scale;
    /// use geo::{LineString, line_string};
    ///
    /// let ls: LineString = line_string![(x: 0., y: 0.), (x: 10., y: 10.)];
    ///
    /// let scaled = ls.scale(2.);
    ///
    /// assert_eq!(scaled, line_string![
    ///     (x: -5., y: -5.),
    ///     (x: 15., y: 15.)
    /// ]);
    /// ```
    #[must_use]
    fn scale(&self, scale_factor: BroadcastablePrimitive<f64>) -> Self;

    // /// Mutable version of [`scale`](Self::scale)
    // fn scale_mut(&mut self, scale_factor: BroadcastablePrimitive<f64>);

    /// Scale a geometry from it's bounding box center, using different values for `x_factor` and
    /// `y_factor` to distort the geometry's [aspect ratio](https://en.wikipedia.org/wiki/Aspect_ratio).
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Scale;
    /// use geo::{LineString, line_string};
    ///
    /// let ls: LineString = line_string![(x: 0., y: 0.), (x: 10., y: 10.)];
    ///
    /// let scaled = ls.scale_xy(2., 4.);
    ///
    /// assert_eq!(scaled, line_string![
    ///     (x: -5., y: -15.),
    ///     (x: 15., y: 25.)
    /// ]);
    /// ```
    #[must_use]
    fn scale_xy(
        &self,
        x_factor: BroadcastablePrimitive<f64>,
        y_factor: BroadcastablePrimitive<f64>,
    ) -> Self;

    // /// Mutable version of [`scale_xy`](Self::scale_xy).
    // fn scale_xy_mut(&mut self, x_factor: BroadcastablePrimitive<f64>, y_factor: BroadcastablePrimitive<f64>);

    /// Scale a geometry around a point of `origin`.
    ///
    /// The point of origin is *usually* given as the 2D bounding box centre of the geometry, in
    /// which case you can just use [`scale`](Self::scale) or [`scale_xy`](Self::scale_xy), but
    /// this method allows you to specify any point.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Scale;
    /// use geo::{LineString, line_string};
    ///
    /// let ls: LineString = line_string![(x: 0., y: 0.), (x: 10., y: 10.)];
    ///
    /// let scaled = ls.scale_xy(2., 4.);
    ///
    /// assert_eq!(scaled, line_string![
    ///     (x: -5., y: -15.),
    ///     (x: 15., y: 25.)
    /// ]);
    /// ```
    #[must_use]
    fn scale_around_point(
        &self,
        x_factor: BroadcastablePrimitive<f64>,
        y_factor: BroadcastablePrimitive<f64>,
        origin: geo::Point,
    ) -> Self;

    // /// Mutable version of [`scale_around_point`](Self::scale_around_point).
    // fn scale_around_point_mut(&mut self, x_factor: BroadcastablePrimitive<f64>, y_factor: BroadcastablePrimitive<f64>, origin: geo::Point);
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ident, $geo_type:ty) => {
        impl Scale for $type {
            fn scale(&self, scale_factor: BroadcastablePrimitive<f64>) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(scale_factor.into_iter())
                    .map(|(maybe_g, scale_factor)| maybe_g.map(|geom| geom.scale(scale_factor)))
                    .collect();

                output_geoms.into()
            }

            fn scale_xy(
                &self,
                x_factor: BroadcastablePrimitive<f64>,
                y_factor: BroadcastablePrimitive<f64>,
            ) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(x_factor.into_iter())
                    .zip(y_factor.into_iter())
                    .map(|((maybe_g, x_factor), y_factor)| {
                        maybe_g.map(|geom| geom.scale_xy(x_factor, y_factor))
                    })
                    .collect();

                output_geoms.into()
            }

            fn scale_around_point(
                &self,
                x_factor: BroadcastablePrimitive<f64>,
                y_factor: BroadcastablePrimitive<f64>,
                origin: geo::Point,
            ) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(x_factor.into_iter())
                    .zip(y_factor.into_iter())
                    .map(|((maybe_g, x_factor), y_factor)| {
                        maybe_g.map(|geom| geom.scale_around_point(x_factor, y_factor, origin))
                    })
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(PointArray, geo::Point);
iter_geo_impl!(LineStringArray, geo::LineString);
iter_geo_impl!(PolygonArray, geo::Polygon);
iter_geo_impl!(MultiPointArray, geo::MultiPoint);
iter_geo_impl!(MultiLineStringArray, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray, geo::MultiPolygon);
iter_geo_impl!(WKBArray, geo::Geometry);

impl Scale for GeometryArray {
    crate::geometry_array_delegate_impl! {
        fn scale(&self, scale_factor: BroadcastablePrimitive<f64>) -> Self;

        fn scale_xy(
            &self,
            x_factor: BroadcastablePrimitive<f64>,
            y_factor: BroadcastablePrimitive<f64>
        ) -> Self;

        fn scale_around_point(
            &self,
            x_factor: BroadcastablePrimitive<f64>,
            y_factor: BroadcastablePrimitive<f64>,
            origin: geo::Point
        ) -> Self;
    }
}
