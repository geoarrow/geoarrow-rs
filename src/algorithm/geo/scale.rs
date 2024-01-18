use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::LineStringArray;
use crate::array::*;
use crate::trait_::GeometryArrayAccessor;
use arrow_array::types::Float64Type;
use arrow_array::OffsetSizeTrait;
use geo::Scale as _Scale;

/// An affine transformation which scales geometries up or down by a factor.
///
/// ## Performance
///
/// If you will be performing multiple transformations, like
/// [`Scale`](crate::algorithm::geo::Scale), [`Skew`](crate::algorithm::geo::Skew),
/// [`Translate`](crate::algorithm::geo::Translate), or [`Rotate`](crate::algorithm::geo::Rotate),
/// it is more efficient to compose the transformations and apply them as a single operation using
/// the [`AffineOps`](crate::algorithm::geo::AffineOps) trait.
pub trait Scale {
    /// Scale geometries from its bounding box center.
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
    fn scale(&self, scale_factor: BroadcastablePrimitive<Float64Type>) -> Self;

    // /// Mutable version of [`scale`](Self::scale)
    // fn scale_mut(&mut self, scale_factor: BroadcastablePrimitive<Float64Type>);

    /// Scale geometries from its bounding box center, using different values for `x_factor` and
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
        x_factor: BroadcastablePrimitive<Float64Type>,
        y_factor: BroadcastablePrimitive<Float64Type>,
    ) -> Self;

    // /// Mutable version of [`scale_xy`](Self::scale_xy).
    // fn scale_xy_mut(&mut self, x_factor: BroadcastablePrimitive<Float64Type>, y_factor: BroadcastablePrimitive<Float64Type>);

    /// Scale geometries around a point of `origin`.
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
        x_factor: BroadcastablePrimitive<Float64Type>,
        y_factor: BroadcastablePrimitive<Float64Type>,
        origin: geo::Point,
    ) -> Self;

    // /// Mutable version of [`scale_around_point`](Self::scale_around_point).
    // fn scale_around_point_mut(&mut self, x_factor: BroadcastablePrimitive<Float64Type>, y_factor: BroadcastablePrimitive<Float64Type>, origin: geo::Point);
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Scale for PointArray {
    fn scale(&self, scale_factor: BroadcastablePrimitive<Float64Type>) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo()
            .zip(&scale_factor)
            .for_each(|(maybe_g, scale_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.scale(scale_factor.unwrap()))
                        .as_ref(),
                )
            });

        output_array.finish()
    }

    fn scale_xy(
        &self,
        x_factor: BroadcastablePrimitive<Float64Type>,
        y_factor: BroadcastablePrimitive<Float64Type>,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo().zip(&x_factor).zip(&y_factor).for_each(
            |((maybe_g, x_factor), y_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.scale_xy(x_factor.unwrap(), y_factor.unwrap()))
                        .as_ref(),
                )
            },
        );

        output_array.finish()
    }

    fn scale_around_point(
        &self,
        x_factor: BroadcastablePrimitive<Float64Type>,
        y_factor: BroadcastablePrimitive<Float64Type>,
        origin: geo::Point,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo().zip(&x_factor).zip(&y_factor).for_each(
            |((maybe_g, x_factor), y_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| {
                            geom.scale_around_point(x_factor.unwrap(), y_factor.unwrap(), origin)
                        })
                        .as_ref(),
                )
            },
        );

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl<O: OffsetSizeTrait> Scale for $type {
            fn scale(&self, scale_factor: BroadcastablePrimitive<Float64Type>) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo()
                    .zip(&scale_factor)
                    .for_each(|(maybe_g, scale_factor)| {
                        output_array
                            .$push_func(
                                maybe_g
                                    .map(|geom| geom.scale(scale_factor.unwrap()))
                                    .as_ref(),
                            )
                            .unwrap();
                    });

                output_array.finish()
            }

            fn scale_xy(
                &self,
                x_factor: BroadcastablePrimitive<Float64Type>,
                y_factor: BroadcastablePrimitive<Float64Type>,
            ) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().zip(&x_factor).zip(&y_factor).for_each(
                    |((maybe_g, x_factor), y_factor)| {
                        output_array
                            .$push_func(
                                maybe_g
                                    .map(|geom| geom.scale_xy(x_factor.unwrap(), y_factor.unwrap()))
                                    .as_ref(),
                            )
                            .unwrap()
                    },
                );

                output_array.finish()
            }

            fn scale_around_point(
                &self,
                x_factor: BroadcastablePrimitive<Float64Type>,
                y_factor: BroadcastablePrimitive<Float64Type>,
                origin: geo::Point,
            ) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().zip(&x_factor).zip(&y_factor).for_each(
                    |((maybe_g, x_factor), y_factor)| {
                        output_array
                            .$push_func(
                                maybe_g
                                    .map(|geom| {
                                        geom.scale_around_point(
                                            x_factor.unwrap(),
                                            y_factor.unwrap(),
                                            origin,
                                        )
                                    })
                                    .as_ref(),
                            )
                            .unwrap()
                    },
                );

                output_array.finish()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>, LineStringBuilder<O>, push_line_string);
iter_geo_impl!(PolygonArray<O>, PolygonBuilder<O>, push_polygon);
iter_geo_impl!(MultiPointArray<O>, MultiPointBuilder<O>, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray<O>,
    MultiLineStringBuilder<O>,
    push_multi_line_string
);
iter_geo_impl!(
    MultiPolygonArray<O>,
    MultiPolygonBuilder<O>,
    push_multi_polygon
);
