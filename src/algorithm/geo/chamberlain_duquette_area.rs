use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
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
    type Output;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output;

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl ChamberlainDuquetteArea for PointArray {
    type Output = Float64Array;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output {
        zeroes(self.len(), self.nulls())
    }

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
        zeroes(self.len(), self.nulls())
    }
}

/// Generate a `ChamberlainDuquetteArea` implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> ChamberlainDuquetteArea for $type {
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

zero_impl!(LineStringArray<O>);
zero_impl!(MultiPointArray<O>);
zero_impl!(MultiLineStringArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> ChamberlainDuquetteArea for $type {
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

iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);
iter_geo_impl!(MixedGeometryArray<O>);
iter_geo_impl!(GeometryCollectionArray<O>);
iter_geo_impl!(WKBArray<O>);

impl ChamberlainDuquetteArea for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn chamberlain_duquette_signed_area(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().chamberlain_duquette_signed_area(),
            GeoDataType::LineString(_) => self.as_line_string().chamberlain_duquette_signed_area(),
            GeoDataType::LargeLineString(_) => self
                .as_large_line_string()
                .chamberlain_duquette_signed_area(),
            GeoDataType::Polygon(_) => self.as_polygon().chamberlain_duquette_signed_area(),
            GeoDataType::LargePolygon(_) => {
                self.as_large_polygon().chamberlain_duquette_signed_area()
            }
            GeoDataType::MultiPoint(_) => self.as_multi_point().chamberlain_duquette_signed_area(),
            GeoDataType::LargeMultiPoint(_) => self
                .as_large_multi_point()
                .chamberlain_duquette_signed_area(),
            GeoDataType::MultiLineString(_) => self
                .as_multi_line_string()
                .chamberlain_duquette_signed_area(),
            GeoDataType::LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .chamberlain_duquette_signed_area(),
            GeoDataType::MultiPolygon(_) => {
                self.as_multi_polygon().chamberlain_duquette_signed_area()
            }
            GeoDataType::LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .chamberlain_duquette_signed_area(),
            GeoDataType::Mixed(_) => self.as_mixed().chamberlain_duquette_signed_area(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().chamberlain_duquette_signed_area(),
            GeoDataType::GeometryCollection(_) => self
                .as_geometry_collection()
                .chamberlain_duquette_signed_area(),
            GeoDataType::LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .chamberlain_duquette_signed_area(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn chamberlain_duquette_unsigned_area(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().chamberlain_duquette_unsigned_area(),
            GeoDataType::LineString(_) => {
                self.as_line_string().chamberlain_duquette_unsigned_area()
            }
            GeoDataType::LargeLineString(_) => self
                .as_large_line_string()
                .chamberlain_duquette_unsigned_area(),
            GeoDataType::Polygon(_) => self.as_polygon().chamberlain_duquette_unsigned_area(),
            GeoDataType::LargePolygon(_) => {
                self.as_large_polygon().chamberlain_duquette_unsigned_area()
            }
            GeoDataType::MultiPoint(_) => {
                self.as_multi_point().chamberlain_duquette_unsigned_area()
            }
            GeoDataType::LargeMultiPoint(_) => self
                .as_large_multi_point()
                .chamberlain_duquette_unsigned_area(),
            GeoDataType::MultiLineString(_) => self
                .as_multi_line_string()
                .chamberlain_duquette_unsigned_area(),
            GeoDataType::LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .chamberlain_duquette_unsigned_area(),
            GeoDataType::MultiPolygon(_) => {
                self.as_multi_polygon().chamberlain_duquette_unsigned_area()
            }
            GeoDataType::LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .chamberlain_duquette_unsigned_area(),
            GeoDataType::Mixed(_) => self.as_mixed().chamberlain_duquette_unsigned_area(),
            GeoDataType::LargeMixed(_) => {
                self.as_large_mixed().chamberlain_duquette_unsigned_area()
            }
            GeoDataType::GeometryCollection(_) => self
                .as_geometry_collection()
                .chamberlain_duquette_unsigned_area(),
            GeoDataType::LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .chamberlain_duquette_unsigned_area(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> ChamberlainDuquetteArea for ChunkedGeometryArray<G> {
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
