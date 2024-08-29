use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, GeoDataType};
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
impl IsValid for PointArray<2> {
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

iter_geos_impl!(LineStringArray<O, 2>);
iter_geos_impl!(MultiPointArray<O, 2>);
iter_geos_impl!(MultiLineStringArray<O, 2>);
iter_geos_impl!(PolygonArray<O, 2>);
iter_geos_impl!(MultiPolygonArray<O, 2>);
iter_geos_impl!(MixedGeometryArray<O, 2>);
iter_geos_impl!(GeometryCollectionArray<O, 2>);
iter_geos_impl!(WKBArray<O>);

impl IsValid for &dyn GeometryArrayTrait {
    type Output = Result<BooleanArray>;

    fn is_valid(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => IsValid::is_valid(self.as_point::<2>()),
            GeoDataType::LineString(_, Dimension::XY) => {
                IsValid::is_valid(self.as_line_string::<2>())
            }
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_line_string::<2>())
            }
            GeoDataType::Polygon(_, Dimension::XY) => IsValid::is_valid(self.as_polygon::<2>()),
            GeoDataType::LargePolygon(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_polygon::<2>())
            }
            GeoDataType::MultiPoint(_, Dimension::XY) => {
                IsValid::is_valid(self.as_multi_point::<2>())
            }
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_multi_point::<2>())
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                IsValid::is_valid(self.as_multi_line_string::<2>())
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_multi_line_string::<2>())
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => {
                IsValid::is_valid(self.as_multi_polygon::<2>())
            }
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_multi_polygon::<2>())
            }
            GeoDataType::Mixed(_, Dimension::XY) => IsValid::is_valid(self.as_mixed::<2>()),
            GeoDataType::LargeMixed(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_mixed::<2>())
            }
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                IsValid::is_valid(self.as_geometry_collection::<2>())
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                IsValid::is_valid(self.as_large_geometry_collection::<2>())
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
