use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedGeometryArrayTrait, ChunkedPointArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;
use geo::algorithm::centroid::Centroid as GeoCentroid;

/// Calculation of the centroid.
///
/// The centroid is the arithmetic mean position of all points in the shape.
/// Informally, it is the point at which a cutout of the shape could be perfectly
/// balanced on the tip of a pin.
/// The geometric centroid of a convex object always lies in the object.
/// A non-convex object might have a centroid that _is outside the object itself_.
///
/// # Examples
///
/// ```
/// use geoarrow::algorithm::geo::Centroid;
/// use geoarrow::array::PolygonArray;
/// use geoarrow::trait_::NativeArrayAccessor;
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
/// let polygon_array: PolygonArray<i32, 2> = vec![polygon].as_slice().into();
///
/// assert_eq!(
///     Some(point!(x: 1., y: 1.)),
///     polygon_array.centroid().get_as_geo(0),
/// );
/// ```
pub trait Centroid {
    type Output;

    /// See: <https://en.wikipedia.org/wiki/Centroid>
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::algorithm::geo::Centroid;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::trait_::NativeArrayAccessor;
    /// use geo::{line_string, point};
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 40.02f64, y: 118.23),
    /// ];
    /// let line_string_array: LineStringArray<i32, 2> = vec![line_string].as_slice().into();
    ///
    /// assert_eq!(
    ///     Some(point!(x: 40.02, y: 117.285)),
    ///     line_string_array.centroid().get_as_geo(0),
    /// );
    /// ```
    fn centroid(&self) -> Self::Output;
}

impl Centroid for PointArray<2> {
    type Output = PointArray<2>;

    fn centroid(&self) -> Self::Output {
        self.clone()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Centroid for $type {
            type Output = PointArray<2>;

            fn centroid(&self) -> Self::Output {
                let mut output_array = PointBuilder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_point(maybe_g.and_then(|g| g.centroid()).as_ref())
                });
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>);
iter_geo_impl!(MixedGeometryArray<O, 2>);
iter_geo_impl!(GeometryCollectionArray<O, 2>);
iter_geo_impl!(WKBArray<O>);

impl Centroid for &dyn NativeArray {
    type Output = Result<PointArray<2>>;

    fn centroid(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().centroid(),
            LineString(_, XY) => self.as_line_string::<2>().centroid(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().centroid(),
            Polygon(_, XY) => self.as_polygon::<2>().centroid(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().centroid(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().centroid(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().centroid(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().centroid(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().centroid(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().centroid(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().centroid(),
            Mixed(_, XY) => self.as_mixed::<2>().centroid(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().centroid(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().centroid(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().centroid(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> Centroid for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedPointArray<2>>;

    fn centroid(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().centroid())?.try_into()
    }
}

impl Centroid for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedPointArray<2>>;

    fn centroid(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().centroid(),
            LineString(_, XY) => self.as_line_string::<2>().centroid(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().centroid(),
            Polygon(_, XY) => self.as_polygon::<2>().centroid(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().centroid(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().centroid(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().centroid(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().centroid(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().centroid(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().centroid(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().centroid(),
            Mixed(_, XY) => self.as_mixed::<2>().centroid(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().centroid(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().centroid(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().centroid(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
