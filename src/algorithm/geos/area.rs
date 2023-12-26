use crate::algorithm::geo::utils::zeroes;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geos::Geom;

/// Unsigned planar area of a geometry.
pub trait Area {
    type Output;

    fn area(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Area for PointArray {
    type Output = Result<Float64Array>;

    fn area(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(zeroes(self.len(), self.nulls()))
            }
        }
    };
}

zero_impl!(LineStringArray<O>);
zero_impl!(MultiPointArray<O>);
zero_impl!(MultiLineStringArray<O>);

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                let mut output_array = Float64Builder::with_capacity(self.len());

                for maybe_g in self.iter_geos() {
                    if let Some(g) = maybe_g {
                        let area = g.area()?;
                        output_array.append_value(area);
                    } else {
                        output_array.append_null();
                    }
                }

                Ok(output_array.finish())
            }
        }
    };
}

iter_geos_impl!(PolygonArray<O>);
iter_geos_impl!(MultiPolygonArray<O>);
iter_geos_impl!(MixedGeometryArray<O>);
iter_geos_impl!(GeometryCollectionArray<O>);
iter_geos_impl!(WKBArray<O>);

impl<O: OffsetSizeTrait> Area for GeometryArray<O> {
    type Output = Result<Float64Array>;

    crate::geometry_array_delegate_impl! {
        fn area(&self) -> Self::Output;
    }
}

impl Area for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn area(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().area(),
            GeoDataType::LineString(_) => self.as_line_string().area(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().area(),
            GeoDataType::Polygon(_) => self.as_polygon().area(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().area(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().area(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().area(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().area(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().area(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().area(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().area(),
            GeoDataType::Mixed(_) => self.as_mixed().area(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().area(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().area(),
            GeoDataType::LargeGeometryCollection(_) => self.as_large_geometry_collection().area(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: GeometryArrayTrait> Area for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn area(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.as_ref().area()?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn tmp() {
        let arr = p_array();
        let area = arr.area().unwrap();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
