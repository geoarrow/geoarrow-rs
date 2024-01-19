use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
use geos::Geom;

/// Checks if the geometry is valid
pub trait IsValid {
    type Output;

    fn is_valid(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl IsValid for PointArray {
    type Output = Result<BooleanArray>;

    fn is_valid(&self) -> Self::Output {
        let mut output_array = BooleanBuilder::with_capacity(self.len());

        for maybe_g in self.iter() {
            if let Some(g) = maybe_g {
                output_array.append_value(g.to_geos()?.is_valid());
            } else {
                output_array.append_null();
            }
        }

        Ok(output_array.finish())
    }
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> IsValid for $type {
            type Output = Result<BooleanArray>;

            fn is_valid(&self) -> Self::Output {
                let mut output_array = BooleanBuilder::with_capacity(self.len());

                for maybe_g in self.iter() {
                    if let Some(g) = maybe_g {
                        output_array.append_value(g.to_geos()?.is_valid());
                    } else {
                        output_array.append_null();
                    }
                }

                Ok(output_array.finish())
            }
        }
    };
}

iter_geos_impl!(LineStringArray<O>);
iter_geos_impl!(MultiPointArray<O>);
iter_geos_impl!(MultiLineStringArray<O>);
iter_geos_impl!(PolygonArray<O>);
iter_geos_impl!(MultiPolygonArray<O>);
iter_geos_impl!(MixedGeometryArray<O>);
iter_geos_impl!(GeometryCollectionArray<O>);
iter_geos_impl!(WKBArray<O>);

impl IsValid for &dyn GeometryArrayTrait {
    type Output = Result<BooleanArray>;

    fn is_valid(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => IsValid::is_valid(self.as_point()),
            GeoDataType::LineString(_) => IsValid::is_valid(self.as_line_string()),
            GeoDataType::LargeLineString(_) => IsValid::is_valid(self.as_large_line_string()),
            GeoDataType::Polygon(_) => IsValid::is_valid(self.as_polygon()),
            GeoDataType::LargePolygon(_) => IsValid::is_valid(self.as_large_polygon()),
            GeoDataType::MultiPoint(_) => IsValid::is_valid(self.as_multi_point()),
            GeoDataType::LargeMultiPoint(_) => IsValid::is_valid(self.as_large_multi_point()),
            GeoDataType::MultiLineString(_) => IsValid::is_valid(self.as_multi_line_string()),
            GeoDataType::LargeMultiLineString(_) => {
                IsValid::is_valid(self.as_large_multi_line_string())
            }
            GeoDataType::MultiPolygon(_) => IsValid::is_valid(self.as_multi_polygon()),
            GeoDataType::LargeMultiPolygon(_) => IsValid::is_valid(self.as_large_multi_polygon()),
            GeoDataType::Mixed(_) => IsValid::is_valid(self.as_mixed()),
            GeoDataType::LargeMixed(_) => IsValid::is_valid(self.as_large_mixed()),
            GeoDataType::GeometryCollection(_) => IsValid::is_valid(self.as_geometry_collection()),
            GeoDataType::LargeGeometryCollection(_) => {
                IsValid::is_valid(self.as_large_geometry_collection())
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: GeometryArrayTrait> IsValid for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_valid(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(IsValid::is_valid(&chunk.as_ref())?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }
}
