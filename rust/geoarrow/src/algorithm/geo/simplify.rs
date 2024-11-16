use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::Simplify as _Simplify;

/// Simplifies a geometry.
///
/// The [Ramer–Douglas–Peucker
/// algorithm](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm) simplifies a
/// linestring. Polygons are simplified by running the RDP algorithm on all their constituent
/// rings. This may result in invalid Polygons, and has no guarantee of preserving topology.
///
/// Multi* objects are simplified by simplifying all their constituent geometries individually.
///
/// An epsilon less than or equal to zero will return an unaltered version of the geometry.
pub trait Simplify {
    type Output;

    /// Returns the simplified representation of a geometry, using the [Ramer–Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm) algorithm
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::algorithm::geo::Simplify;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::trait_::ArrayAccessor;
    /// use geo::line_string;
    /// use geoarrow::datatypes::Dimension;
    ///
    /// let line_string = line_string![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 5.0, y: 4.0),
    ///     (x: 11.0, y: 5.5),
    ///     (x: 17.3, y: 3.2),
    ///     (x: 27.8, y: 0.1),
    /// ];
    /// let line_string_array: LineStringArray = (vec![line_string].as_slice(), Dimension::XY).into();
    ///
    /// let simplified_array = line_string_array.simplify(&1.0);
    ///
    /// let expected = line_string![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 5.0, y: 4.0),
    ///     (x: 11.0, y: 5.5),
    ///     (x: 27.8, y: 0.1),
    /// ];
    ///
    /// assert_eq!(expected, simplified_array.value_as_geo(0))
    /// ```
    fn simplify(&self, epsilon: &f64) -> Self::Output;
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl Simplify for $type {
            type Output = Self;

            fn simplify(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(PointArray);
identity_impl!(MultiPointArray);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl Simplify for $type {
            type Output = Self;

            fn simplify(&self, epsilon: &f64) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.simplify(epsilon)))
                    .collect();

                (output_geoms, Dimension::XY).into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray, geo::LineString);
iter_geo_impl!(PolygonArray, geo::Polygon);
iter_geo_impl!(MultiLineStringArray, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray, geo::MultiPolygon);
// iter_geo_impl!(MixedGeometryArray, geo::Geometry);
// iter_geo_impl!(GeometryCollectionArray, geo::GeometryCollection);

impl Simplify for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn simplify(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().simplify(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string().simplify(epsilon)),
            Polygon(_, XY) => Arc::new(self.as_polygon().simplify(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().simplify(epsilon)),
            MultiLineString(_, XY) => Arc::new(self.as_multi_line_string().simplify(epsilon)),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().simplify(epsilon)),
            // Mixed(_, XY) => self.as_mixed().simplify(epsilon),
            // GeometryCollection(_, XY) => self.as_geometry_collection().simplify(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl Simplify for ChunkedGeometryArray<PointArray> {
    type Output = Self;

    fn simplify(&self, epsilon: &f64) -> Self::Output {
        self.map(|chunk| chunk.simplify(epsilon))
            .try_into()
            .unwrap()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl Simplify for $type {
            type Output = Self;

            fn simplify(&self, epsilon: &f64) -> Self {
                self.map(|chunk| chunk.simplify(epsilon))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray>);
chunked_impl!(ChunkedGeometryArray<PolygonArray>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray>);

impl Simplify for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn simplify(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().simplify(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string().simplify(epsilon)),
            Polygon(_, XY) => Arc::new(self.as_polygon().simplify(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().simplify(epsilon)),
            MultiLineString(_, XY) => Arc::new(self.as_multi_line_string().simplify(epsilon)),
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().simplify(epsilon)),
            // Mixed(_, XY) => self.as_mixed().simplify(epsilon),
            // GeometryCollection(_, XY) => self.as_geometry_collection().simplify(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{LineStringArray, PolygonArray};
    use crate::trait_::ArrayAccessor;
    use geo::{line_string, polygon};

    #[test]
    fn rdp_test() {
        let input_geom = line_string![
            (x: 0.0, y: 0.0 ),
            (x: 5.0, y: 4.0 ),
            (x: 11.0, y: 5.5 ),
            (x: 17.3, y: 3.2 ),
            (x: 27.8, y: 0.1 ),
        ];
        let input_array: LineStringArray = (vec![input_geom].as_slice(), Dimension::XY).into();
        let result_array = input_array.simplify(&1.0);

        let expected = line_string![
            ( x: 0.0, y: 0.0 ),
            ( x: 5.0, y: 4.0 ),
            ( x: 11.0, y: 5.5 ),
            ( x: 27.8, y: 0.1 ),
        ];

        assert_eq!(expected, result_array.get_as_geo(0).unwrap());
    }

    #[test]
    fn polygon() {
        let input_geom = polygon![
            (x: 0., y: 0.),
            (x: 0., y: 10.),
            (x: 5., y: 11.),
            (x: 10., y: 10.),
            (x: 10., y: 0.),
            (x: 0., y: 0.),
        ];
        let input_array: PolygonArray = (vec![input_geom].as_slice(), Dimension::XY).into();
        let result_array = input_array.simplify(&2.0);

        let expected = polygon![
            (x: 0., y: 0.),
            (x: 0., y: 10.),
            (x: 10., y: 10.),
            (x: 10., y: 0.),
            (x: 0., y: 0.),
        ];

        assert_eq!(expected, result_array.get_as_geo(0).unwrap());
    }
}
