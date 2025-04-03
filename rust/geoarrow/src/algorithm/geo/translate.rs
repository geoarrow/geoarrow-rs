use std::sync::Arc;

use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::types::Float64Type;
use geo::Translate as _Translate;
use geoarrow_schema::Dimension;

pub trait Translate {
    type Output;

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
        x_offset: &BroadcastablePrimitive<Float64Type>,
        y_offset: &BroadcastablePrimitive<Float64Type>,
    ) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Translate for PointArray {
    type Output = Self;

    fn translate(
        &self,
        x_offset: &BroadcastablePrimitive<Float64Type>,
        y_offset: &BroadcastablePrimitive<Float64Type>,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(Dimension::XY, self.buffer_lengths());

        self.iter_geo()
            .zip(x_offset)
            .zip(y_offset)
            .for_each(|((maybe_g, x_offset), y_offset)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.translate(x_offset.unwrap(), y_offset.unwrap()))
                        .as_ref(),
                )
            });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl Translate for $type {
            type Output = Self;

            fn translate(
                &self,
                x_offset: &BroadcastablePrimitive<Float64Type>,
                y_offset: &BroadcastablePrimitive<Float64Type>,
            ) -> Self {
                let mut output_array =
                    <$builder_type>::with_capacity(Dimension::XY, self.buffer_lengths());

                self.iter_geo().zip(x_offset).zip(y_offset).for_each(
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

iter_geo_impl!(LineStringArray, LineStringBuilder, push_line_string);
iter_geo_impl!(PolygonArray, PolygonBuilder, push_polygon);
iter_geo_impl!(MultiPointArray, MultiPointBuilder, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray,
    MultiLineStringBuilder,
    push_multi_line_string
);
iter_geo_impl!(MultiPolygonArray, MultiPolygonBuilder, push_multi_polygon);

impl Translate for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn translate(
        &self,
        x_offset: &BroadcastablePrimitive<Float64Type>,
        y_offset: &BroadcastablePrimitive<Float64Type>,
    ) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().translate(x_offset, y_offset))
            }};
        }

        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_) => impl_method!(as_point),
            LineString(_) => impl_method!(as_line_string),
            Polygon(_) => impl_method!(as_polygon),
            MultiPoint(_) => impl_method!(as_multi_point),
            MultiLineString(_) => impl_method!(as_multi_line_string),
            MultiPolygon(_) => impl_method!(as_multi_polygon),
            // Mixed(_) => impl_method!(as_mixed),
            // GeometryCollection(_) => impl_method!(as_geometry_collection),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }
}
