use crate::array::*;
use arrow_array::OffsetSizeTrait;
use geo::SimplifyVw as _SimplifyVw;

/// Simplifies a geometry.
///
/// Polygons are simplified by running the algorithm on all their constituent rings.  This may
/// result in invalid Polygons, and has no guarantee of preserving topology. Multi* objects are
/// simplified by simplifying all their constituent geometries individually.
///
/// An epsilon less than or equal to zero will return an unaltered version of the geometry.
pub trait SimplifyVw {
    /// Returns the simplified representation of a geometry, using the [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263) algorithm
    ///
    /// See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::algorithm::geo::SimplifyVw;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::trait_::GeometryArrayAccessor;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 3.0, y: 8.0),
    ///     (x: 6.0, y: 20.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    /// let line_string_array: LineStringArray<i32> = vec![line_string].as_slice().into();
    ///
    /// let simplified_array = line_string_array.simplify_vw(&30.0);
    ///
    /// let expected = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    ///
    /// assert_eq!(expected, simplified_array.value_as_geo(0))
    /// ```
    fn simplify_vw(&self, epsilon: &f64) -> Self;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl SimplifyVw for PointArray {
    fn simplify_vw(&self, _epsilon: &f64) -> Self {
        self.clone()
    }
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVw for $type {
            fn simplify_vw(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVw for $type {
            fn simplify_vw(&self, epsilon: &f64) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.simplify_vw(epsilon)))
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

impl<O: OffsetSizeTrait> SimplifyVw for GeometryArray<O> {
    fn simplify_vw(&self, epsilon: &f64) -> Self {
        use GeometryArray::*;

        match self {
            Point(arr) => Point(arr.simplify_vw(epsilon)),
            LineString(arr) => LineString(arr.simplify_vw(epsilon)),
            Polygon(arr) => Polygon(arr.simplify_vw(epsilon)),
            MultiPoint(arr) => MultiPoint(arr.simplify_vw(epsilon)),
            MultiLineString(arr) => MultiLineString(arr.simplify_vw(epsilon)),
            MultiPolygon(arr) => MultiPolygon(arr.simplify_vw(epsilon)),
            Rect(arr) => Rect(arr.clone()),
        }
    }
}
