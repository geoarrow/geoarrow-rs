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

/// Returns `true` if the geometry is a ring.
pub trait IsRing {
    type Output;

    fn is_ring(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl IsRing for PointArray {
    type Output = Result<BooleanArray>;

    fn is_ring(&self) -> Self::Output {
        let mut output_array = BooleanBuilder::with_capacity(self.len());

        for maybe_g in self.iter() {
            if let Some(g) = maybe_g {
                output_array.append_value(g.to_geos()?.is_ring()?);
            } else {
                output_array.append_null();
            }
        }

        Ok(output_array.finish())
    }
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> IsRing for $type {
            type Output = Result<BooleanArray>;

            fn is_ring(&self) -> Self::Output {
                let mut output_array = BooleanBuilder::with_capacity(self.len());

                for maybe_g in self.iter() {
                    if let Some(g) = maybe_g {
                        output_array.append_value(g.to_geos()?.is_ring()?);
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

impl IsRing for &dyn GeometryArrayTrait {
    type Output = Result<BooleanArray>;

    fn is_ring(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().is_ring(),
            GeoDataType::LineString(_) => self.as_line_string().is_ring(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().is_ring(),
            GeoDataType::Polygon(_) => self.as_polygon().is_ring(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().is_ring(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().is_ring(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().is_ring(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().is_ring(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().is_ring(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().is_ring(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().is_ring(),
            GeoDataType::Mixed(_) => self.as_mixed().is_ring(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().is_ring(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().is_ring(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().is_ring()
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: GeometryArrayTrait> IsRing for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_ring(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.as_ref().is_ring()?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }
}
