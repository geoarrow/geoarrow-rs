use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
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
        x_offset: BroadcastablePrimitive<f64>,
        y_offset: BroadcastablePrimitive<f64>,
    ) -> Self;

    // /// Translate a Geometry along its axes, but in place.
    // fn translate_mut(&mut self, x_offset: T, y_offset: T);
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Translate for PointArray {
    fn translate(
        &self,
        x_offset: BroadcastablePrimitive<f64>,
        y_offset: BroadcastablePrimitive<f64>,
    ) -> Self {
        let output_geoms: Vec<Option<geo::Point>> = self
            .iter_geo()
            .zip(&x_offset)
            .zip(&y_offset)
            .map(|((maybe_g, x_offset), y_offset)| {
                maybe_g.map(|geom| geom.translate(x_offset, y_offset))
            })
            .collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> Translate for $type {
            fn translate(
                &self,
                x_offset: BroadcastablePrimitive<f64>,
                y_offset: BroadcastablePrimitive<f64>,
            ) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(x_offset.into_iter())
                    .zip(y_offset.into_iter())
                    .map(|((maybe_g, x_offset), y_offset)| {
                        maybe_g.map(|geom| geom.translate(x_offset, y_offset))
                    })
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>, geo::LineString);
iter_geo_impl!(PolygonArray<O>, geo::Polygon);
iter_geo_impl!(MultiPointArray<O>, geo::MultiPoint);
iter_geo_impl!(MultiLineStringArray<O>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O>, geo::MultiPolygon);
iter_geo_impl!(WKBArray<O>, geo::Geometry);

impl<O: OffsetSizeTrait> Translate for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn translate(
            &self,
            x_offset: BroadcastablePrimitive<f64>,
            y_offset: BroadcastablePrimitive<f64>
        ) -> Self;
    }
}
