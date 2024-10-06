use crate::algorithm::native::binary::try_binary_primitive_native_geometry;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::GeometryTrait;
use crate::io::geo::geometry_to_geo;
use crate::trait_::NativeGeometryAccessor;
use crate::trait_::NativeScalar;
use arrow_array::Float64Array;
use geo::EuclideanDistance as _EuclideanDistance;

pub trait EuclideanDistance<'a, Rhs> {
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
    fn euclidean_distance(&'a self, rhs: &'a Rhs) -> Result<Float64Array>;
}

macro_rules! iter_geo_impl {
    ($array_type:ty) => {
        impl<'a, R: NativeGeometryAccessor<'a, 2>> EuclideanDistance<'a, R> for $array_type {
            fn euclidean_distance(&'a self, rhs: &'a R) -> Result<Float64Array> {
                try_binary_primitive_native_geometry(self, rhs, |l, r| {
                    Ok(l.to_geo().euclidean_distance(&r.to_geo()))
                })
            }
        }
    };
}

iter_geo_impl!(PointArray<2>);
iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);
iter_geo_impl!(RectArray<2>);

impl<'a, R: NativeGeometryAccessor<'a, 2>> EuclideanDistance<'a, R> for &dyn NativeArray {
    fn euclidean_distance(&'a self, rhs: &'a R) -> Result<Float64Array> {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => EuclideanDistance::euclidean_distance(self.as_point::<2>(), rhs),
            LineString(_, XY) => {
                EuclideanDistance::euclidean_distance(self.as_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => EuclideanDistance::euclidean_distance(self.as_polygon::<2>(), rhs),
            MultiPoint(_, XY) => {
                EuclideanDistance::euclidean_distance(self.as_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                EuclideanDistance::euclidean_distance(self.as_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => {
                EuclideanDistance::euclidean_distance(self.as_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => EuclideanDistance::euclidean_distance(self.as_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                EuclideanDistance::euclidean_distance(self.as_geometry_collection::<2>(), rhs)
            }
            Rect(XY) => EuclideanDistance::euclidean_distance(self.as_rect::<2>(), rhs),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait EuclideanDistanceScalar<'a, G: GeometryTrait> {
    fn euclidean_distance(&'a self, rhs: &'a G) -> Result<Float64Array>;
}

macro_rules! scalar_impl {
    ($array_type:ty) => {
        impl<'a, G: GeometryTrait<T = f64>> EuclideanDistanceScalar<'a, G> for $array_type {
            fn euclidean_distance(&'a self, rhs: &'a G) -> Result<Float64Array> {
                let right = geometry_to_geo(rhs);
                self.try_unary_primitive(|left| {
                    Ok::<_, GeoArrowError>(left.to_geo().euclidean_distance(&right))
                })
            }
        }
    };
}

scalar_impl!(PointArray<2>);
scalar_impl!(LineStringArray<2>);
scalar_impl!(PolygonArray<2>);
scalar_impl!(MultiPointArray<2>);
scalar_impl!(MultiLineStringArray<2>);
scalar_impl!(MultiPolygonArray<2>);
scalar_impl!(MixedGeometryArray<2>);
scalar_impl!(GeometryCollectionArray<2>);
scalar_impl!(RectArray<2>);

impl<'a, G: GeometryTrait<T = f64>> EuclideanDistanceScalar<'a, G> for &dyn NativeArray {
    fn euclidean_distance(&'a self, rhs: &'a G) -> Result<Float64Array> {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => EuclideanDistanceScalar::euclidean_distance(self.as_point::<2>(), rhs),
            LineString(_, XY) => {
                EuclideanDistanceScalar::euclidean_distance(self.as_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => {
                EuclideanDistanceScalar::euclidean_distance(self.as_polygon::<2>(), rhs)
            }
            MultiPoint(_, XY) => {
                EuclideanDistanceScalar::euclidean_distance(self.as_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                EuclideanDistanceScalar::euclidean_distance(self.as_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => {
                EuclideanDistanceScalar::euclidean_distance(self.as_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => EuclideanDistanceScalar::euclidean_distance(self.as_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                EuclideanDistanceScalar::euclidean_distance(self.as_geometry_collection::<2>(), rhs)
            }
            Rect(XY) => EuclideanDistanceScalar::euclidean_distance(self.as_rect::<2>(), rhs),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
