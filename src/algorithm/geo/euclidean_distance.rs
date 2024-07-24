use crate::algorithm::native::Binary;
use crate::array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::Result;
use crate::trait_::{GeometryArrayTrait, GeometryScalarTrait};
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::EuclideanDistance as _EuclideanDistance;

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

pub trait EuclideanDistance<Rhs = Self> {
    type Output;

    /// Returns the distance between two geometries
    ///
    /// If a `Point` is contained by a `Polygon`, the distance is `0.0`
    ///
    /// If a `Point` lies on a `Polygon`'s exterior or interior rings, the distance is `0.0`
    ///
    /// If a `Point` lies on a `LineString`, the distance is `0.0`
    ///
    /// The distance between a `Point` and an empty `LineString` is `0.0`
    ///
    /// # Examples
    ///
    /// `Point` to `Point`:
    ///
    /// ```
    /// use approx::assert_relative_eq;
    /// use geo::EuclideanDistance;
    /// use geo::point;
    ///
    /// let p1 = point!(x: -72.1235, y: 42.3521);
    /// let p2 = point!(x: -72.1260, y: 42.45);
    ///
    /// let distance = p1.euclidean_distance(&p2);
    ///
    /// assert_relative_eq!(distance, 0.09793191512474639);
    /// ```
    ///
    /// `Point` to `Polygon`:
    ///
    /// ```
    /// use approx::assert_relative_eq;
    /// use geo::EuclideanDistance;
    /// use geo::{point, polygon};
    ///
    /// let polygon = polygon![
    ///     (x: 5., y: 1.),
    ///     (x: 4., y: 2.),
    ///     (x: 4., y: 3.),
    ///     (x: 5., y: 4.),
    ///     (x: 6., y: 4.),
    ///     (x: 7., y: 3.),
    ///     (x: 7., y: 2.),
    ///     (x: 6., y: 1.),
    ///     (x: 5., y: 1.),
    /// ];
    ///
    /// let point = point!(x: 2.5, y: 0.5);
    ///
    /// let distance = point.euclidean_distance(&polygon);
    ///
    /// assert_relative_eq!(distance, 2.1213203435596424);
    /// ```
    ///
    /// `Point` to `LineString`:
    ///
    /// ```
    /// use approx::assert_relative_eq;
    /// use geo::EuclideanDistance;
    /// use geo::{point, line_string};
    ///
    /// let line_string = line_string![
    ///     (x: 5., y: 1.),
    ///     (x: 4., y: 2.),
    ///     (x: 4., y: 3.),
    ///     (x: 5., y: 4.),
    ///     (x: 6., y: 4.),
    ///     (x: 7., y: 3.),
    ///     (x: 7., y: 2.),
    ///     (x: 6., y: 1.),
    /// ];
    ///
    /// let point = point!(x: 5.5, y: 2.1);
    ///
    /// let distance = point.euclidean_distance(&line_string);
    ///
    /// assert_relative_eq!(distance, 1.1313708498984762);
    /// ```
    fn euclidean_distance(&self, rhs: &Rhs) -> Self::Output;
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl EuclideanDistance<PointArray<2>> for PointArray<2> {
    type Output = Result<Float64Array>;

    /// Minimum distance between two Points
    fn euclidean_distance(&self, rhs: &PointArray<2>) -> Self::Output {
        self.try_binary_primitive(rhs, |left, right| {
            Ok(left.to_geo().euclidean_distance(&right.to_geo()))
        })
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> EuclideanDistance<$second> for $first {
            type Output = Result<Float64Array>;

            fn euclidean_distance(&self, rhs: &$second) -> Self::Output {
                self.try_binary_primitive(rhs, |left, right| {
                    Ok(left.to_geo().euclidean_distance(&right.to_geo()))
                })
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(PointArray<2>, LineStringArray<O,2 >);
iter_geo_impl!(PointArray<2>, PolygonArray<O,2 >);
iter_geo_impl!(PointArray<2>, MultiPointArray<O,2 >);
iter_geo_impl!(PointArray<2>, MultiLineStringArray<O,2 >);
iter_geo_impl!(PointArray<2>, MultiPolygonArray<O,2 >);
iter_geo_impl!(PointArray<2>, MixedGeometryArray<O,2 >);
iter_geo_impl!(PointArray<2>, GeometryCollectionArray<O,2 >);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray<O,2 >, PointArray<2>);
iter_geo_impl!(LineStringArray<O,2 >, LineStringArray<O,2 >);
iter_geo_impl!(LineStringArray<O,2 >, PolygonArray<O,2 >);
iter_geo_impl!(LineStringArray<O,2 >, MultiPointArray<O,2 >);
iter_geo_impl!(LineStringArray<O,2 >, MultiLineStringArray<O,2 >);
iter_geo_impl!(LineStringArray<O,2 >, MultiPolygonArray<O,2 >);
iter_geo_impl!(LineStringArray<O,2 >, MixedGeometryArray<O,2 >);
iter_geo_impl!(LineStringArray<O,2 >, GeometryCollectionArray<O,2 >);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<O,2 >, PointArray<2>);
iter_geo_impl!(PolygonArray<O,2 >, LineStringArray<O,2 >);
iter_geo_impl!(PolygonArray<O,2 >, PolygonArray<O,2 >);
iter_geo_impl!(PolygonArray<O,2 >, MultiPointArray<O,2 >);
iter_geo_impl!(PolygonArray<O,2 >, MultiLineStringArray<O,2 >);
iter_geo_impl!(PolygonArray<O,2 >, MultiPolygonArray<O,2 >);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<O,2 >, PointArray<2>);
iter_geo_impl!(MultiPointArray<O,2 >, LineStringArray<O,2 >);
iter_geo_impl!(MultiPointArray<O,2 >, PolygonArray<O,2 >);
iter_geo_impl!(MultiPointArray<O,2 >, MultiPointArray<O,2 >);
iter_geo_impl!(MultiPointArray<O,2 >, MultiLineStringArray<O,2 >);
iter_geo_impl!(MultiPointArray<O,2 >, MultiPolygonArray<O,2 >);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<O,2 >, PointArray<2>);
iter_geo_impl!(MultiLineStringArray<O,2 >, LineStringArray<O,2 >);
iter_geo_impl!(MultiLineStringArray<O,2 >, PolygonArray<O,2 >);
iter_geo_impl!(MultiLineStringArray<O,2 >, MultiPointArray<O,2 >);
iter_geo_impl!(MultiLineStringArray<O,2 >, MultiLineStringArray<O,2 >);
iter_geo_impl!(MultiLineStringArray<O,2 >, MultiPolygonArray<O,2 >);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<O,2 >, PointArray<2>);
iter_geo_impl!(MultiPolygonArray<O,2 >, LineStringArray<O,2 >);
iter_geo_impl!(MultiPolygonArray<O,2 >, PolygonArray<O,2 >);
iter_geo_impl!(MultiPolygonArray<O,2 >, MultiPointArray<O,2 >);
iter_geo_impl!(MultiPolygonArray<O,2 >, MultiLineStringArray<O,2 >);
iter_geo_impl!(MultiPolygonArray<O,2 >, MultiPolygonArray<O,2 >);

impl EuclideanDistance for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn euclidean_distance(&self, rhs: &Self) -> Self::Output {
        use GeoDataType::*;
        match (self.data_type(), rhs.data_type()) {
            (Point(_, Dimension::XY), Point(_, Dimension::XY)) => {
                self.as_point_2d().euclidean_distance(rhs.as_point_2d())
            }
            (Point(_, Dimension::XY), LineString(_, Dimension::XY)) => self
                .as_point_2d()
                .euclidean_distance(rhs.as_line_string_2d()),
            (Point(_, Dimension::XY), LargeLineString(_, Dimension::XY)) => self
                .as_point_2d()
                .euclidean_distance(rhs.as_line_string_2d()),
            (Point(_, Dimension::XY), Polygon(_, Dimension::XY)) => {
                self.as_point_2d().euclidean_distance(rhs.as_polygon_2d())
            }
            (Point(_, Dimension::XY), MultiPoint(_, Dimension::XY)) => self
                .as_point_2d()
                .euclidean_distance(rhs.as_multi_point_2d()),
            (Point(_, Dimension::XY), MultiLineString(_, Dimension::XY)) => self
                .as_point_2d()
                .euclidean_distance(rhs.as_multi_line_string_2d()),
            (Point(_, Dimension::XY), MultiPolygon(_, Dimension::XY)) => self
                .as_point_2d()
                .euclidean_distance(rhs.as_multi_polygon_2d()),
            (Point(_, Dimension::XY), Mixed(_, Dimension::XY)) => {
                self.as_point_2d().euclidean_distance(rhs.as_mixed_2d())
            }
            (Point(_, Dimension::XY), GeometryCollection(_, Dimension::XY)) => self
                .as_point_2d()
                .euclidean_distance(rhs.as_geometry_collection_2d()),

            _ => todo!(),
        }
    }
}

// // ┌─────────────────────────────────┐
// // │ Implementations for RHS scalars │
// // └─────────────────────────────────┘

// // Note: this implementation is outside the macro because it is not generic over O
// impl<'a> EuclideanDistance<Point<'a>> for PointArray {
//     /// Minimum distance between two Points
//     fn euclidean_distance(&self, other: &Point<'a>) -> Float64Array {
//         let mut output_array = Float64Builder::with_capacity(self.len());

//         self.iter_geo().for_each(|maybe_point| {
//             let output = maybe_point.map(|point| point.euclidean_distance(&other.to_geo()));
//             output_array.append_option(output)
//         });

//         output_array.finish()
//     }
// }

// /// Implementation that iterates over geo objects
// macro_rules! iter_geo_impl_scalar {
//     ($first:ty, $second:ty) => {
//         impl<'a, O: OffsetSizeTrait> EuclideanDistance<$second> for $first {
//             fn euclidean_distance(&self, other: &$second) -> Float64Array {
//                 let mut output_array = Float64Builder::with_capacity(self.len());
//                 let other_geo = other.to_geo();

//                 self.iter_geo().for_each(|maybe_geom| {
//                     let output = maybe_geom.map(|geom| geom.euclidean_distance(&other_geo));
//                     output_array.append_option(output)
//                 });

//                 output_array.finish()
//             }
//         }
//     };
// }

// // Implementations on PointArray
// iter_geo_impl_scalar!(PointArray, LineString<'a, O>);
// iter_geo_impl_scalar!(PointArray, Polygon<'a, O>);
// iter_geo_impl_scalar!(PointArray, MultiPoint<'a, O>);
// iter_geo_impl_scalar!(PointArray, MultiLineString<'a, O>);
// iter_geo_impl_scalar!(PointArray, MultiPolygon<'a, O>);

// // Implementations on LineStringArray
// iter_geo_impl_scalar!(LineStringArray<O>, Point<'a>);
// iter_geo_impl_scalar!(LineStringArray<O>, LineString<'a, O>);
// iter_geo_impl_scalar!(LineStringArray<O>, Polygon<'a, O>);
// // iter_geo_impl_scalar!(LineStringArray<O>, MultiPoint<'a, O>);
// // iter_geo_impl_scalar!(LineStringArray<O>, MultiLineString<'a, O>);
// // iter_geo_impl_scalar!(LineStringArray<O>, MultiPolygon<'a, O>);

// // Implementations on PolygonArray
// iter_geo_impl_scalar!(PolygonArray<O>, Point<'a>);
// iter_geo_impl_scalar!(PolygonArray<O>, LineString<'a, O>);
// iter_geo_impl_scalar!(PolygonArray<O>, Polygon<'a, O>);
// // iter_geo_impl_scalar!(PolygonArray<O>, MultiPoint<'a, O>);
// // iter_geo_impl_scalar!(PolygonArray<O>, MultiLineString<'a, O>);
// // iter_geo_impl_scalar!(PolygonArray<O>, MultiPolygon<'a, O>);

// // Implementations on MultiPointArray
// iter_geo_impl_scalar!(MultiPointArray<O>, Point<'a>);
// // iter_geo_impl_scalar!(MultiPointArray<O>, LineString<'a, O>);
// // iter_geo_impl_scalar!(MultiPointArray<O>, Polygon<'a, O>);
// // iter_geo_impl_scalar!(MultiPointArray<O>, MultiPoint<'a, O>);
// // iter_geo_impl_scalar!(MultiPointArray<O>, MultiLineString<'a, O>);
// // iter_geo_impl_scalar!(MultiPointArray<O>, MultiPolygon<'a, O>);

// // Implementations on MultiLineStringArray
// iter_geo_impl_scalar!(MultiLineStringArray<O>, Point<'a>);
// // iter_geo_impl_scalar!(MultiLineStringArray<O>, LineString<'a, O>);
// // iter_geo_impl_scalar!(MultiLineStringArray<O>, Polygon<'a, O>);
// // iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiPoint<'a, O>);
// // iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiLineString<'a, O>);
// // iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiPolygon<'a, O>);

// // Implementations on MultiPolygonArray
// iter_geo_impl_scalar!(MultiPolygonArray<O>, Point<'a>);
// // iter_geo_impl_scalar!(MultiPolygonArray<O>, LineString<'a, O>);
// // iter_geo_impl_scalar!(MultiPolygonArray<O>, Polygon<'a, O>);
// // iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiPoint<'a, O>);
// // iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiLineString<'a, O>);
// // iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiPolygon<'a, O>);
