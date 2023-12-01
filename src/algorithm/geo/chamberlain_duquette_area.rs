use crate::algorithm::geo::utils::zeroes;
use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
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
/// use geoarrow::array::PolygonArray;
/// use geoarrow::GeometryArrayTrait;
/// use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
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
/// let mut reversed_polygon = polygon.clone();
/// reversed_polygon.exterior_mut(|line_string| {
///     line_string.0.reverse();
/// });
///
/// let polygon_array: PolygonArray<i32> = vec![polygon].as_slice().into();
/// let reversed_polygon_array: PolygonArray<i32> = vec![reversed_polygon].as_slice().into();
///
/// // 78,478 meters²
/// assert_eq!(78_478., polygon_array.chamberlain_duquette_unsigned_area().value(0).round());
/// assert_eq!(78_478., polygon_array.chamberlain_duquette_signed_area().value(0).round());
///
/// assert_eq!(78_478., reversed_polygon_array.chamberlain_duquette_unsigned_area().value(0).round());
/// assert_eq!(-78_478., reversed_polygon_array.chamberlain_duquette_signed_area().value(0).round());
/// ```
pub trait ChamberlainDuquetteArea {
    fn chamberlain_duquette_signed_area(&self) -> Float64Array;

    fn chamberlain_duquette_unsigned_area(&self) -> Float64Array;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl ChamberlainDuquetteArea for PointArray {
    fn chamberlain_duquette_signed_area(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }

    fn chamberlain_duquette_unsigned_area(&self) -> Float64Array {
        zeroes(self.len(), self.nulls())
    }
}

/// Generate a `ChamberlainDuquetteArea` implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> ChamberlainDuquetteArea for $type {
            fn chamberlain_duquette_signed_area(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
            }

            fn chamberlain_duquette_unsigned_area(&self) -> Float64Array {
                zeroes(self.len(), self.nulls())
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
        impl<O: OffsetSizeTrait> ChamberlainDuquetteArea for $type {
            fn chamberlain_duquette_signed_area(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array
                        .append_option(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
                });
                output_array.finish()
            }

            fn chamberlain_duquette_unsigned_area(&self) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array
                        .append_option(maybe_g.map(|g| g.chamberlain_duquette_unsigned_area()))
                });
                output_array.finish()
            }
        }
    };
}

iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(WKBArray<O>);

impl<O: OffsetSizeTrait> ChamberlainDuquetteArea for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn chamberlain_duquette_signed_area(&self) -> Float64Array;
        fn chamberlain_duquette_unsigned_area(&self) -> Float64Array;
    }
}
