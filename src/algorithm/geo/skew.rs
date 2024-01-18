use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::LineStringArray;
use crate::array::*;
use crate::trait_::GeometryArrayAccessor;
use arrow_array::types::Float64Type;
use arrow_array::OffsetSizeTrait;
use geo::Skew as _Skew;

/// An affine transformation which skews a geometry, sheared by angles along x and y dimensions.
///
/// ## Performance
///
/// If you will be performing multiple transformations, like
/// [`Scale`](crate::algorithm::geo::Scale), [`Skew`](crate::algorithm::geo::Skew),
/// [`Translate`](crate::algorithm::geo::Translate), or [`Rotate`](crate::algorithm::geo::Rotate),
/// it is more efficient to compose the transformations and apply them as a single operation using
/// the [`AffineOps`](crate::algorithm::geo::AffineOps) trait.
pub trait Skew {
    /// An affine transformation which skews a geometry, sheared by a uniform angle along the x and
    /// y dimensions.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Skew;
    /// use geo::{Polygon, polygon};
    ///
    /// let square: Polygon = polygon![
    ///     (x: 0., y: 0.),
    ///     (x: 10., y: 0.),
    ///     (x: 10., y: 10.),
    ///     (x: 0., y: 10.)
    /// ];
    ///
    /// let skewed = square.skew(30.);
    ///
    /// let expected_output: Polygon = polygon![
    ///     (x: -2.89, y: -2.89),
    ///     (x: 7.11, y: 2.89),
    ///     (x: 12.89, y: 12.89),
    ///     (x: 2.89, y: 7.11)
    /// ];
    /// approx::assert_relative_eq!(skewed, expected_output, epsilon = 1e-2);
    /// ```
    #[must_use]
    fn skew(&self, degrees: BroadcastablePrimitive<Float64Type>) -> Self;

    // /// Mutable version of [`skew`](Self::skew).
    // fn skew_mut(&mut self, degrees: BroadcastablePrimitive<Float64Type>);

    /// An affine transformation which skews a geometry, sheared by an angle along the x and y dimensions.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Skew;
    /// use geo::{Polygon, polygon};
    ///
    /// let square: Polygon = polygon![
    ///     (x: 0., y: 0.),
    ///     (x: 10., y: 0.),
    ///     (x: 10., y: 10.),
    ///     (x: 0., y: 10.)
    /// ];
    ///
    /// let skewed = square.skew_xy(30., 12.);
    ///
    /// let expected_output: Polygon = polygon![
    ///     (x: -2.89, y: -1.06),
    ///     (x: 7.11, y: 1.06),
    ///     (x: 12.89, y: 11.06),
    ///     (x: 2.89, y: 8.94)
    /// ];
    /// approx::assert_relative_eq!(skewed, expected_output, epsilon = 1e-2);
    /// ```
    #[must_use]
    fn skew_xy(
        &self,
        degrees_x: BroadcastablePrimitive<Float64Type>,
        degrees_y: BroadcastablePrimitive<Float64Type>,
    ) -> Self;

    // /// Mutable version of [`skew_xy`](Self::skew_xy).
    // fn skew_xy_mut(
    //     &mut self,
    //     degrees_x: BroadcastablePrimitive<Float64Type>,
    //     degrees_y: BroadcastablePrimitive<Float64Type>,
    // );

    /// An affine transformation which skews a geometry around a point of `origin`, sheared by an
    /// angle along the x and y dimensions.
    ///
    /// The point of origin is *usually* given as the 2D bounding box centre of the geometry, in
    /// which case you can just use [`skew`](Self::skew) or [`skew_xy`](Self::skew_xy), but this method allows you
    /// to specify any point.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Skew;
    /// use geo::{Polygon, polygon, point};
    ///
    /// let square: Polygon = polygon![
    ///     (x: 0., y: 0.),
    ///     (x: 10., y: 0.),
    ///     (x: 10., y: 10.),
    ///     (x: 0., y: 10.)
    /// ];
    ///
    /// let origin = point! { x: 2., y: 2. };
    /// let skewed = square.skew_around_point(45.0, 10.0, origin);
    ///
    /// let expected_output: Polygon = polygon![
    ///     (x: -2., y: -0.353),
    ///     (x: 8., y: 1.410),
    ///     (x: 18., y: 11.41),
    ///     (x: 8., y: 9.647)
    /// ];
    /// approx::assert_relative_eq!(skewed, expected_output, epsilon = 1e-2);
    /// ```
    #[must_use]
    fn skew_around_point(
        &self,
        degrees_x: BroadcastablePrimitive<Float64Type>,
        degrees_y: BroadcastablePrimitive<Float64Type>,
        origin: geo::Point,
    ) -> Self;

    // /// Mutable version of [`skew_around_point`](Self::skew_around_point).
    // fn skew_around_point_mut(
    //     &mut self,
    //     degrees_x: BroadcastablePrimitive<Float64Type>,
    //     degrees_y: BroadcastablePrimitive<Float64Type>,
    //     origin: geo::Point,
    // );
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Skew for PointArray {
    fn skew(&self, scale_factor: BroadcastablePrimitive<Float64Type>) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo()
            .zip(&scale_factor)
            .for_each(|(maybe_g, scale_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.skew(scale_factor.unwrap()))
                        .as_ref(),
                )
            });

        output_array.finish()
    }

    fn skew_xy(
        &self,
        x_factor: BroadcastablePrimitive<Float64Type>,
        y_factor: BroadcastablePrimitive<Float64Type>,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo().zip(&x_factor).zip(&y_factor).for_each(
            |((maybe_g, x_factor), y_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.skew_xy(x_factor.unwrap(), y_factor.unwrap()))
                        .as_ref(),
                )
            },
        );

        output_array.finish()
    }

    fn skew_around_point(
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
                            geom.skew_around_point(x_factor.unwrap(), y_factor.unwrap(), origin)
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
        impl<O: OffsetSizeTrait> Skew for $type {
            fn skew(&self, scale_factor: BroadcastablePrimitive<Float64Type>) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo()
                    .zip(&scale_factor)
                    .for_each(|(maybe_g, scale_factor)| {
                        output_array
                            .$push_func(
                                maybe_g
                                    .map(|geom| geom.skew(scale_factor.unwrap()))
                                    .as_ref(),
                            )
                            .unwrap();
                    });

                output_array.finish()
            }

            fn skew_xy(
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
                                    .map(|geom| geom.skew_xy(x_factor.unwrap(), y_factor.unwrap()))
                                    .as_ref(),
                            )
                            .unwrap()
                    },
                );

                output_array.finish()
            }

            fn skew_around_point(
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
                                        geom.skew_around_point(
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
