use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::prelude::GeodesicArea as _GeodesicArea;

/// Determine the perimeter and area of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)].
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
pub trait GeodesicArea {
    /// Determine the area of a geometry on an ellipsoidal model of the earth.
    ///
    /// This uses the geodesic measurement methods given by [Karney (2013)].
    ///
    /// # Assumptions
    ///  - Polygons are assumed to be wound in a counter-clockwise direction
    ///    for the exterior ring and a clockwise direction for interior rings.
    ///    This is the standard winding for geometries that follow the Simple Feature standard.
    ///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
    ///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
    ///    with polygons larger than this, please use the `unsigned` methods.
    ///
    /// # Units
    ///
    /// - return value: meter²
    ///
    /// # Interpreting negative area values
    ///
    /// A negative value can mean one of two things:
    /// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
    /// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the `unsigned` methods.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::{polygon, Polygon};
    /// use geoarrow::array::PolygonArray;
    /// use geoarrow::algorithm::geo::GeodesicArea;
    ///
    /// // The O2 in London
    /// let polygon: Polygon<f64> = polygon![
    ///     (x: 0.00388383, y: 51.501574),
    ///     (x: 0.00538587, y: 51.502278),
    ///     (x: 0.00553607, y: 51.503299),
    ///     (x: 0.00467777, y: 51.504181),
    ///     (x: 0.00327229, y: 51.504435),
    ///     (x: 0.00187754, y: 51.504168),
    ///     (x: 0.00087976, y: 51.503380),
    ///     (x: 0.00107288, y: 51.502324),
    ///     (x: 0.00185608, y: 51.501770),
    ///     (x: 0.00388383, y: 51.501574),
    /// ];
    /// let polygon_array: PolygonArray<i32> = vec![polygon].as_slice().into();
    ///
    /// let area_array = polygon_array.geodesic_area_signed();
    ///
    /// assert_eq!(
    ///     78_596., // meters
    ///     area_array.value(0).round()
    /// );
    /// ```
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_area_signed(&self) -> Float64Array;

    /// Determine the area of a geometry on an ellipsoidal model of the earth. Supports very large geometries that cover a significant portion of the earth.
    ///
    /// This uses the geodesic measurement methods given by [Karney (2013)].
    ///
    /// # Assumptions
    ///  - Polygons are assumed to be wound in a counter-clockwise direction
    ///    for the exterior ring and a clockwise direction for interior rings.
    ///    This is the standard winding for geometries that follow the Simple Features standard.
    ///    Using alternative windings will result in incorrect results.
    ///
    /// # Units
    ///
    /// - return value: meter²
    ///
    /// # Examples
    /// ```rust
    /// use geo::{polygon, Polygon};
    /// use geoarrow::array::PolygonArray;
    /// use geoarrow::algorithm::geo::GeodesicArea;
    ///
    /// // Describe a polygon that covers all of the earth EXCEPT this small square.
    /// // The outside of the polygon is in this square, the inside of the polygon is the rest of the earth.
    /// let polygon: Polygon<f64> = polygon![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 0.0, y: 1.0),
    ///     (x: 1.0, y: 1.0),
    ///     (x: 1.0, y: 0.0),
    /// ];
    /// let polygon_array: PolygonArray<i32> = vec![polygon].as_slice().into();
    ///
    /// let area_array = polygon_array.geodesic_area_unsigned();
    ///
    /// // Over 5 trillion square meters!
    /// assert_eq!(area_array.value(0), 510053312945726.94);
    /// ```
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_area_unsigned(&self) -> Float64Array;

    /// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
    ///
    /// This uses the geodesic measurement methods given by [Karney (2013)].
    ///
    /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
    /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
    ///
    /// # Units
    ///
    /// - return value: meter
    ///
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_perimeter(&self) -> Float64Array;

    /// Determine the perimeter and area of a geometry on an ellipsoidal model of the earth, all in one operation.
    ///
    /// This returns the perimeter and area in a `(perimeter, area)` tuple and uses the geodesic measurement methods given by [Karney (2013)].
    ///
    /// # Area Assumptions
    ///  - Polygons are assumed to be wound in a counter-clockwise direction
    ///    for the exterior ring and a clockwise direction for interior rings.
    ///    This is the standard winding for Geometries that follow the Simple Features standard.
    ///    Alternative windings may result in a negative area. See "Interpreting negative area values" below.
    ///  - Polygons are assumed to be smaller than half the size of the earth. If you expect to be dealing
    ///    with polygons larger than this, please use the 'unsigned' methods.
    ///
    /// # Perimeter
    /// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
    /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
    ///
    /// # Units
    ///
    /// - return value: (meter, meter²)
    ///
    /// # Interpreting negative area values
    ///
    /// A negative area value can mean one of two things:
    /// 1. The winding of the polygon is in the clockwise direction (reverse winding). If this is the case, and you know the polygon is smaller than half the area of earth, you can take the absolute value of the reported area to get the correct area.
    /// 2. The polygon is larger than half the planet. In this case, the returned area of the polygon is not correct. If you expect to be dealing with very large polygons, please use the 'unsigned' methods.
    ///
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_perimeter_area_signed(&self) -> (Float64Array, Float64Array);

    /// Determine the perimeter and area of a geometry on an ellipsoidal model of the earth, all in one operation. Supports very large geometries that cover a significant portion of the earth.
    ///
    /// This returns the perimeter and area in a `(perimeter, area)` tuple and uses the geodesic measurement methods given by [Karney (2013)].
    ///
    /// # Area Assumptions
    ///  - Polygons are assumed to be wound in a counter-clockwise direction
    ///    for the exterior ring and a clockwise direction for interior rings.
    ///    This is the standard winding for Geometries that follow the Simple Features standard.
    ///    Using alternative windings will result in incorrect results.
    ///
    /// # Perimeter
    /// For a polygon this returns the perimeter of the exterior ring and interior rings.
    /// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
    ///
    /// # Units
    ///
    /// - return value: (meter, meter²)
    ///
    /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
    fn geodesic_perimeter_area_unsigned(&self) -> (Float64Array, Float64Array);
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl GeodesicArea for PointArray {
    fn geodesic_perimeter(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }

    fn geodesic_area_signed(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }

    fn geodesic_area_unsigned(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }

    fn geodesic_perimeter_area_signed(&self) -> (Float64Array, Float64Array) {
        (
            zeroes(self.len(), self.nulls()),
            zeroes(self.len(), self.nulls()),
        )
    }

    fn geodesic_perimeter_area_unsigned(&self) -> (Float64Array, Float64Array) {
        (
            zeroes(self.len(), self.nulls()),
            zeroes(self.len(), self.nulls()),
        )
    }
}

/// Generate a `GeodesicArea` implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicArea for $type {
            fn geodesic_perimeter(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }

            fn geodesic_area_signed(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }

            fn geodesic_area_unsigned(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }

            fn geodesic_perimeter_area_signed(&self) -> (Float64Array, Float64Array) {
                (
                    zeroes(self.len(), self.nulls()),
                    zeroes(self.len(), self.nulls()),
                )
            }

            fn geodesic_perimeter_area_unsigned(&self) -> (Float64Array, Float64Array) {
                (
                    zeroes(self.len(), self.nulls()),
                    zeroes(self.len(), self.nulls()),
                )
            }
        }
    };
}

zero_impl!(LineStringArray<O>);
zero_impl!(MultiPointArray<O>);
zero_impl!(MultiLineStringArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> GeodesicArea for $type {
            fn geodesic_perimeter(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.geodesic_perimeter()))
                });

                output_array.finish()
            }

            fn geodesic_area_signed(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.geodesic_area_signed()))
                });

                output_array.finish()
            }

            fn geodesic_area_unsigned(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_g| {
                    output_array.append_option(maybe_g.map(|g| g.geodesic_area_unsigned()))
                });

                output_array.finish()
            }

            fn geodesic_perimeter_area_signed(&self) -> (Float64Array, Float64Array) {
                let mut output_perimeter_array = Float64Builder::with_capacity(self.len());
                let mut output_area_array = Float64Builder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_g| {
                    if let Some(g) = maybe_g {
                        let (perimeter, area) = g.geodesic_perimeter_area_signed();
                        output_perimeter_array.append_value(perimeter);
                        output_area_array.append_value(area);
                    } else {
                        output_perimeter_array.append_null();
                        output_area_array.append_null();
                    }
                });

                (output_perimeter_array.finish(), output_area_array.finish())
            }

            fn geodesic_perimeter_area_unsigned(&self) -> (Float64Array, Float64Array) {
                let mut output_perimeter_array = Float64Builder::with_capacity(self.len());
                let mut output_area_array = Float64Builder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_g| {
                    if let Some(g) = maybe_g {
                        let (perimeter, area) = g.geodesic_perimeter_area_unsigned();
                        output_perimeter_array.append_value(perimeter);
                        output_area_array.append_value(area);
                    } else {
                        output_perimeter_array.append_null();
                        output_area_array.append_null();
                    }
                });

                (output_perimeter_array.finish(), output_area_array.finish())
            }
        }
    };
}

iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(WKBArray<O>);

impl<O: OffsetSizeTrait> GeodesicArea for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn geodesic_area_signed(&self) -> Float64Array;
        fn geodesic_area_unsigned(&self) -> Float64Array;
        fn geodesic_perimeter(&self) -> Float64Array;
        fn geodesic_perimeter_area_signed(&self) -> (Float64Array, Float64Array);
        fn geodesic_perimeter_area_unsigned(&self) -> (Float64Array, Float64Array);
    }
}
