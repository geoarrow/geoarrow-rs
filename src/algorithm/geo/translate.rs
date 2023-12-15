use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
use arrow_array::types::Float64Type;
use arrow_array::OffsetSizeTrait;
use geo::Translate as _Translate;

pub trait Translate {
    /// Translate a Geometry along its axes by the given offsets
    ///
    /// ## Performance
    ///
    /// If you will be performing multiple transformations, like
    /// [`Scale`](crate::algorithm::geo::Scale), [`Skew`](crate::algorithm::geo::Skew),
    /// [`Translate`](crate::algorithm::geo::Translate), or
    /// [`Rotate`](crate::algorithm::geo::Rotate), it is more efficient to compose the
    /// transformations and apply them as a single operation using the
    /// [`AffineOps`](crate::algorithm::geo::AffineOps) trait.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Translate;
    /// use geo::line_string;
    ///
    /// let ls = line_string![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 5.0, y: 5.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    ///
    /// let translated = ls.translate(1.5, 3.5);
    ///
    /// assert_eq!(translated, line_string![
    ///     (x: 1.5, y: 3.5),
    ///     (x: 6.5, y: 8.5),
    ///     (x: 11.5, y: 13.5),
    /// ]);
    /// ```
    #[must_use]
    fn translate(
        &self,
        x_offset: BroadcastablePrimitive<Float64Type>,
        y_offset: BroadcastablePrimitive<Float64Type>,
    ) -> Self;

    // /// Translate a Geometry along its axes, but in place.
    // fn translate_mut(&mut self, x_offset: T, y_offset: T);
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Translate for PointArray {
    fn translate(
        &self,
        x_offset: BroadcastablePrimitive<Float64Type>,
        y_offset: BroadcastablePrimitive<Float64Type>,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo().zip(&x_offset).zip(&y_offset).for_each(
            |((maybe_g, x_offset), y_offset)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.translate(x_offset.unwrap(), y_offset.unwrap()))
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
        impl<O: OffsetSizeTrait> Translate for $type {
            fn translate(
                &self,
                x_offset: BroadcastablePrimitive<Float64Type>,
                y_offset: BroadcastablePrimitive<Float64Type>,
            ) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().zip(&x_offset).zip(&y_offset).for_each(
                    |((maybe_g, x_offset), y_offset)| {
                        output_array
                            .$push_func(
                                maybe_g
                                    .map(|geom| {
                                        geom.translate(x_offset.unwrap(), y_offset.unwrap())
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

impl<O: OffsetSizeTrait> Translate for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn translate(
            &self,
            x_offset: BroadcastablePrimitive<Float64Type>,
            y_offset: BroadcastablePrimitive<Float64Type>
        ) -> Self;
    }
}
