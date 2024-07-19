use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedGeometryArrayTrait, ChunkedPointArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
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
/// use geoarrow::trait_::GeometryArrayAccessor;
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
/// let polygon_array: PolygonArray<i32> = vec![polygon].as_slice().into();
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
    /// use geoarrow::trait_::GeometryArrayAccessor;
    /// use geo::{line_string, point};
    ///
    /// let line_string = line_string![
    ///     (x: 40.02f64, y: 116.34),
    ///     (x: 40.02f64, y: 118.23),
    /// ];
    /// let line_string_array: LineStringArray<i32> = vec![line_string].as_slice().into();
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

impl Centroid for &dyn GeometryArrayTrait {
    type Output = Result<PointArray<2>>;

    fn centroid(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().centroid(),
            GeoDataType::LineString(_) => self.as_line_string().centroid(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().centroid(),
            GeoDataType::Polygon(_) => self.as_polygon().centroid(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().centroid(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().centroid(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().centroid(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().centroid(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().centroid(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().centroid(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().centroid(),
            GeoDataType::Mixed(_) => self.as_mixed().centroid(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().centroid(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().centroid(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().centroid()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> Centroid for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedPointArray<2>>;

    fn centroid(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().centroid())?.try_into()
    }
}

impl Centroid for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<ChunkedPointArray<2>>;

    fn centroid(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().centroid(),
            GeoDataType::LineString(_) => self.as_line_string().centroid(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().centroid(),
            GeoDataType::Polygon(_) => self.as_polygon().centroid(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().centroid(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().centroid(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().centroid(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().centroid(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().centroid(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().centroid(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().centroid(),
            GeoDataType::Mixed(_) => self.as_mixed().centroid(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().centroid(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().centroid(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().centroid()
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
