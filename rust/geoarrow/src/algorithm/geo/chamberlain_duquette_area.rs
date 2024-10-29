use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::builder::Float64Builder;
use arrow_array::Float64Array;
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
/// use geoarrow::NativeArray;
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
/// let polygon_array: PolygonArray<2> = vec![polygon].as_slice().into();
/// let reversed_polygon_array: PolygonArray<2> = vec![reversed_polygon].as_slice().into();
///
/// // 78,478 meters²
/// assert_eq!(78_478., polygon_array.chamberlain_duquette_unsigned_area().value(0).round());
/// assert_eq!(78_478., polygon_array.chamberlain_duquette_signed_area().value(0).round());
///
/// assert_eq!(78_478., reversed_polygon_array.chamberlain_duquette_unsigned_area().value(0).round());
/// assert_eq!(-78_478., reversed_polygon_array.chamberlain_duquette_signed_area().value(0).round());
/// ```
pub trait ChamberlainDuquetteArea {
    type Output;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output;

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output;
}

/// Generate a `ChamberlainDuquetteArea` implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl ChamberlainDuquetteArea for $type {
            type Output = Float64Array;

            fn chamberlain_duquette_signed_area(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }

            fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(PointArray<2>);
zero_impl!(LineStringArray<2>);
zero_impl!(MultiPointArray<2>);
zero_impl!(MultiLineStringArray<2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl ChamberlainDuquetteArea for $type {
            type Output = Float64Array;

            fn chamberlain_duquette_signed_area(&self) -> Self::Output {
                let mut output_array = Float64Builder::with_capacity(self.len());
                self.iter_geo().for_each(|maybe_g| {
                    output_array
                        .append_option(maybe_g.map(|g| g.chamberlain_duquette_signed_area()))
                });
                output_array.finish()
            }

            fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
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

iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);

impl ChamberlainDuquetteArea for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().chamberlain_duquette_signed_area(),
            LineString(_, XY) => self
                .as_line_string::<2>()
                .chamberlain_duquette_signed_area(),
            Polygon(_, XY) => self.as_polygon::<2>().chamberlain_duquette_signed_area(),
            MultiPoint(_, XY) => self
                .as_multi_point::<2>()
                .chamberlain_duquette_signed_area(),
            MultiLineString(_, XY) => self
                .as_multi_line_string::<2>()
                .chamberlain_duquette_signed_area(),
            MultiPolygon(_, XY) => self
                .as_multi_polygon::<2>()
                .chamberlain_duquette_signed_area(),
            Mixed(_, XY) => self.as_mixed::<2>().chamberlain_duquette_signed_area(),
            GeometryCollection(_, XY) => self
                .as_geometry_collection::<2>()
                .chamberlain_duquette_signed_area(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().chamberlain_duquette_unsigned_area(),
            LineString(_, XY) => self
                .as_line_string::<2>()
                .chamberlain_duquette_unsigned_area(),
            Polygon(_, XY) => self.as_polygon::<2>().chamberlain_duquette_unsigned_area(),
            MultiPoint(_, XY) => self
                .as_multi_point::<2>()
                .chamberlain_duquette_unsigned_area(),
            MultiLineString(_, XY) => self
                .as_multi_line_string::<2>()
                .chamberlain_duquette_unsigned_area(),
            MultiPolygon(_, XY) => self
                .as_multi_polygon::<2>()
                .chamberlain_duquette_unsigned_area(),
            Mixed(_, XY) => self.as_mixed::<2>().chamberlain_duquette_unsigned_area(),
            GeometryCollection(_, XY) => self
                .as_geometry_collection::<2>()
                .chamberlain_duquette_unsigned_area(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> ChamberlainDuquetteArea for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.as_ref().chamberlain_duquette_signed_area()?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.as_ref().chamberlain_duquette_unsigned_area()?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }
}

impl ChamberlainDuquetteArea for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().chamberlain_duquette_signed_area(),
            LineString(_, XY) => self
                .as_line_string::<2>()
                .chamberlain_duquette_signed_area(),
            Polygon(_, XY) => self.as_polygon::<2>().chamberlain_duquette_signed_area(),
            MultiPoint(_, XY) => self
                .as_multi_point::<2>()
                .chamberlain_duquette_signed_area(),
            MultiLineString(_, XY) => self
                .as_multi_line_string::<2>()
                .chamberlain_duquette_signed_area(),
            MultiPolygon(_, XY) => self
                .as_multi_polygon::<2>()
                .chamberlain_duquette_signed_area(),
            Mixed(_, XY) => self.as_mixed::<2>().chamberlain_duquette_signed_area(),
            GeometryCollection(_, XY) => self
                .as_geometry_collection::<2>()
                .chamberlain_duquette_signed_area(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().chamberlain_duquette_unsigned_area(),
            LineString(_, XY) => self
                .as_line_string::<2>()
                .chamberlain_duquette_unsigned_area(),
            Polygon(_, XY) => self.as_polygon::<2>().chamberlain_duquette_unsigned_area(),
            MultiPoint(_, XY) => self
                .as_multi_point::<2>()
                .chamberlain_duquette_unsigned_area(),
            MultiLineString(_, XY) => self
                .as_multi_line_string::<2>()
                .chamberlain_duquette_unsigned_area(),
            MultiPolygon(_, XY) => self
                .as_multi_polygon::<2>()
                .chamberlain_duquette_unsigned_area(),
            Mixed(_, XY) => self.as_mixed::<2>().chamberlain_duquette_unsigned_area(),
            GeometryCollection(_, XY) => self
                .as_geometry_collection::<2>()
                .chamberlain_duquette_unsigned_area(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
