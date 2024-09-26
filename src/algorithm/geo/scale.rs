use std::sync::Arc;

use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::LineStringArray;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
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
pub trait Scale: Sized {
    type Output;

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
    fn scale(&self, scale_factor: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        self.scale_xy(scale_factor, scale_factor)
    }

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
        x_factor: &BroadcastablePrimitive<Float64Type>,
        y_factor: &BroadcastablePrimitive<Float64Type>,
    ) -> Self::Output;

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
        x_factor: &BroadcastablePrimitive<Float64Type>,
        y_factor: &BroadcastablePrimitive<Float64Type>,
        origin: geo::Point,
    ) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Scale for PointArray<2> {
    type Output = Self;

    fn scale_xy(
        &self,
        x_factor: &BroadcastablePrimitive<Float64Type>,
        y_factor: &BroadcastablePrimitive<Float64Type>,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo()
            .zip(x_factor)
            .zip(y_factor)
            .for_each(|((maybe_g, x_factor), y_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.scale_xy(x_factor.unwrap(), y_factor.unwrap()))
                        .as_ref(),
                )
            });

        output_array.finish()
    }

    fn scale_around_point(
        &self,
        x_factor: &BroadcastablePrimitive<Float64Type>,
        y_factor: &BroadcastablePrimitive<Float64Type>,
        origin: geo::Point,
    ) -> Self {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo()
            .zip(x_factor)
            .zip(y_factor)
            .for_each(|((maybe_g, x_factor), y_factor)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| {
                            geom.scale_around_point(x_factor.unwrap(), y_factor.unwrap(), origin)
                        })
                        .as_ref(),
                )
            });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl<O: OffsetSizeTrait> Scale for $type {
            type Output = Self;

            fn scale_xy(
                &self,
                x_factor: &BroadcastablePrimitive<Float64Type>,
                y_factor: &BroadcastablePrimitive<Float64Type>,
            ) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().zip(x_factor).zip(y_factor).for_each(
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
                x_factor: &BroadcastablePrimitive<Float64Type>,
                y_factor: &BroadcastablePrimitive<Float64Type>,
                origin: geo::Point,
            ) -> Self {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().zip(x_factor).zip(y_factor).for_each(
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

iter_geo_impl!(LineStringArray<O, 2>, LineStringBuilder<O, 2>, push_line_string);
iter_geo_impl!(PolygonArray<O, 2>, PolygonBuilder<O, 2>, push_polygon);
iter_geo_impl!(MultiPointArray<O, 2>, MultiPointBuilder<O, 2>, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray<O, 2>,
    MultiLineStringBuilder<O, 2>,
    push_multi_line_string
);
iter_geo_impl!(
    MultiPolygonArray<O, 2>,
    MultiPolygonBuilder<O, 2>,
    push_multi_polygon
);

impl Scale for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn scale_xy(
        &self,
        x_factor: &BroadcastablePrimitive<Float64Type>,
        y_factor: &BroadcastablePrimitive<Float64Type>,
    ) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().scale_xy(x_factor, y_factor))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            LargeLineString(_, XY) => impl_method!(as_large_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            LargePolygon(_, XY) => impl_method!(as_large_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            LargeMultiPoint(_, XY) => impl_method!(as_large_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            LargeMultiLineString(_, XY) => {
                impl_method!(as_large_multi_line_string)
            }
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            LargeMultiPolygon(_, XY) => impl_method!(as_large_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // LargeMixed(_, XY) => impl_method!(as_large_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // LargeGeometryCollection(_, XY) => {
            //     impl_method!(as_large_geometry_collection)
            // }
            // WKB => impl_method!(as_wkb),
            // LargeWKB => impl_method!(as_large_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn scale_around_point(
        &self,
        x_factor: &BroadcastablePrimitive<Float64Type>,
        y_factor: &BroadcastablePrimitive<Float64Type>,
        origin: geo::Point,
    ) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(
                    self.$method()
                        .scale_around_point(x_factor, y_factor, origin),
                )
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            LargeLineString(_, XY) => impl_method!(as_large_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            LargePolygon(_, XY) => impl_method!(as_large_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            LargeMultiPoint(_, XY) => impl_method!(as_large_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            LargeMultiLineString(_, XY) => {
                impl_method!(as_large_multi_line_string)
            }
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            LargeMultiPolygon(_, XY) => impl_method!(as_large_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // LargeMixed(_, XY) => impl_method!(as_large_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // LargeGeometryCollection(_, XY) => {
            //     impl_method!(as_large_geometry_collection)
            // }
            // WKB => impl_method!(as_wkb),
            // LargeWKB => impl_method!(as_large_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }
}
