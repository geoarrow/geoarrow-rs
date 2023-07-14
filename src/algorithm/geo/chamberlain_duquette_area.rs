use crate::algorithm::geo::utils::zeroes;
use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use geo::prelude::ChamberlainDuquetteArea as GeoChamberlainDuquetteArea;

/// Calculate the signed approximate geodesic area of a `Geometry`.
///
/// # Units
///
/// - return value: meters²
///
/// # References
///
/// * Robert. G. Chamberlain and William H. Duquette, "Some Algorithms for Polygons on a Sphere",
///
///   JPL Publication 07-03, Jet Propulsion Laboratory, Pasadena, CA, June 2007 <https://trs.jpl.nasa.gov/handle/2014/41271>
///
/// # Examples
///
/// ```
/// use geo::{polygon, Polygon};
/// use geo::chamberlain_duquette_area::ChamberlainDuquetteArea;
///
/// // The O2 in London
/// let mut polygon: Polygon<f64> = polygon![
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
///
/// // 78,478 meters²
/// assert_eq!(78_478., polygon.chamberlain_duquette_unsigned_area().round());
/// assert_eq!(78_478., polygon.chamberlain_duquette_signed_area().round());
///
/// polygon.exterior_mut(|line_string| {
///     line_string.0.reverse();
/// });
///
/// assert_eq!(78_478., polygon.chamberlain_duquette_unsigned_area().round());
/// assert_eq!(-78_478., polygon.chamberlain_duquette_signed_area().round());
/// ```
pub trait ChamberlainDuquetteArea {
    fn chamberlain_duquette_signed_area(&self) -> PrimitiveArray<f64>;

    fn chamberlain_duquette_unsigned_area(&self) -> PrimitiveArray<f64>;
}

/// Generate a `ChamberlainDuquetteArea` implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ident) => {
        impl ChamberlainDuquetteArea for $type {
            fn chamberlain_duquette_signed_area(&self) -> PrimitiveArray<f64> {
                zeroes(self.len(), self.validity())
            }

            fn chamberlain_duquette_unsigned_area(&self) -> PrimitiveArray<f64> {
                zeroes(self.len(), self.validity())
            }
        }
    };
}

zero_impl!(PointArray);
zero_impl!(LineStringArray);
zero_impl!(MultiPointArray);
zero_impl!(MultiLineStringArray);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ident) => {
        impl ChamberlainDuquetteArea for $type {
            fn chamberlain_duquette_signed_area(&self) -> PrimitiveArray<f64> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
                });
                output_array.into()
            }

            fn chamberlain_duquette_unsigned_area(&self) -> PrimitiveArray<f64> {
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
                });
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(WKBArray);

impl ChamberlainDuquetteArea for GeometryArray {
    crate::geometry_array_delegate_impl! {
        fn chamberlain_duquette_signed_area(&self) -> PrimitiveArray<f64>;
        fn chamberlain_duquette_unsigned_area(&self) -> PrimitiveArray<f64>;
    }
}
