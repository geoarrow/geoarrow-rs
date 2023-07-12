use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    MutablePointArray, PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use geo::algorithm::centroid::Centroid as GeoCentroid;

/// Calculation of the centroid.
/// The centroid is the arithmetic mean position of all points in the shape.
/// Informally, it is the point at which a cutout of the shape could be perfectly
/// balanced on the tip of a pin.
/// The geometric centroid of a convex object always lies in the object.
/// A non-convex object might have a centroid that _is outside the object itself_.
///
/// # Examples
///
/// ```
/// use geo::Centroid;
/// use geo::{point, polygon};
///
/// // rhombus shaped polygon
/// let polygon = polygon![
///     (x: -2., y: 1.),
///     (x: 1., y: 3.),
///     (x: 4., y: 1.),
///     (x: 1., y: -1.),
///     (x: -2., y: 1.),
/// ];
///
/// assert_eq!(
///     Some(point!(x: 1., y: 1.)),
///     polygon.centroid(),
/// );
/// ```
pub trait Centroid {
    /// See: <https://en.wikipedia.org/wiki/Centroid>
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Centroid;
    /// use geo::{line_string, point};
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 40.02f64, y: 118.23),
    /// ];
    ///
    /// assert_eq!(
    ///     Some(point!(x: 40.02, y: 117.285)),
    ///     line_string.centroid(),
    /// );
    /// ```
    fn centroid(&self) -> PointArray;
}

impl Centroid for PointArray {
    fn centroid(&self) -> PointArray {
        self.clone()
    }
}

impl Centroid for LineStringArray {
    fn centroid(&self) -> PointArray {
        let mut output_array = MutablePointArray::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        output_array.into()
    }
}

impl Centroid for PolygonArray {
    fn centroid(&self) -> PointArray {
        let mut output_array = MutablePointArray::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        output_array.into()
    }
}

impl Centroid for MultiPointArray {
    fn centroid(&self) -> PointArray {
        let mut output_array = MutablePointArray::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        output_array.into()
    }
}

impl Centroid for MultiLineStringArray {
    fn centroid(&self) -> PointArray {
        let mut output_array = MutablePointArray::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        output_array.into()
    }
}

impl Centroid for MultiPolygonArray {
    fn centroid(&self) -> PointArray {
        let mut output_array = MutablePointArray::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        output_array.into()
    }
}

impl Centroid for WKBArray {
    fn centroid(&self) -> PointArray {
        let mut output_array = MutablePointArray::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        output_array.into()
    }
}

impl Centroid for GeometryArray {
    fn centroid(&self) -> PointArray {
        match self {
            GeometryArray::WKB(arr) => arr.centroid(),
            GeometryArray::Point(arr) => arr.centroid(),
            GeometryArray::LineString(arr) => arr.centroid(),
            GeometryArray::Polygon(arr) => arr.centroid(),
            GeometryArray::MultiPoint(arr) => arr.centroid(),
            GeometryArray::MultiLineString(arr) => arr.centroid(),
            GeometryArray::MultiPolygon(arr) => arr.centroid(),
        }
    }
}
