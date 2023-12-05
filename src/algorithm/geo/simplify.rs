use crate::array::*;
use arrow_array::OffsetSizeTrait;
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
    /// Returns the simplified representation of a geometry, using the [Ramer–Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm) algorithm
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::algorithm::geo::Simplify;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::trait_::GeometryArrayAccessor;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 5.0, y: 4.0),
    ///     (x: 11.0, y: 5.5),
    ///     (x: 17.3, y: 3.2),
    ///     (x: 27.8, y: 0.1),
    /// ];
    /// let line_string_array: LineStringArray<i32> = vec![line_string].as_slice().into();
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
    fn simplify(&self, epsilon: &f64) -> Self;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Simplify for PointArray {
    fn simplify(&self, _epsilon: &f64) -> Self {
        self.clone()
    }
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Simplify for $type {
            fn simplify(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> Simplify for $type {
            fn simplify(&self, epsilon: &f64) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.simplify(epsilon)))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>, geo::LineString);
iter_geo_impl!(PolygonArray<O>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<O>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O>, geo::MultiPolygon);

impl<O: OffsetSizeTrait> Simplify for GeometryArray<O> {
    fn simplify(&self, epsilon: &f64) -> Self {
        use GeometryArray::*;

        match self {
            Point(arr) => Point(arr.simplify(epsilon)),
            LineString(arr) => LineString(arr.simplify(epsilon)),
            Polygon(arr) => Polygon(arr.simplify(epsilon)),
            MultiPoint(arr) => MultiPoint(arr.simplify(epsilon)),
            MultiLineString(arr) => MultiLineString(arr.simplify(epsilon)),
            MultiPolygon(arr) => MultiPolygon(arr.simplify(epsilon)),
            Rect(arr) => Rect(arr.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{LineStringArray, PolygonArray};
    use crate::trait_::GeometryArrayAccessor;
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
        let input_array: LineStringArray<i64> = vec![input_geom].as_slice().into();
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
        let input_array: PolygonArray<i64> = vec![input_geom].as_slice().into();
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
