use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::GeometryScalarTrait;
use crate::NativeArray;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
use geos::Geom;

/// Returns `true` if the geometry is a ring.
pub trait IsRing {
    type Output;

    fn is_ring(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl IsRing for PointArray<2> {
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

iter_geos_impl!(LineStringArray<O, 2>);
iter_geos_impl!(MultiPointArray<O, 2>);
iter_geos_impl!(MultiLineStringArray<O, 2>);
iter_geos_impl!(PolygonArray<O, 2>);
iter_geos_impl!(MultiPolygonArray<O, 2>);
iter_geos_impl!(MixedGeometryArray<O, 2>);
iter_geos_impl!(GeometryCollectionArray<O, 2>);
iter_geos_impl!(WKBArray<O>);

impl IsRing for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_ring(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().is_ring(),
            LineString(_, XY) => self.as_line_string::<2>().is_ring(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().is_ring(),
            Polygon(_, XY) => self.as_polygon::<2>().is_ring(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().is_ring(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().is_ring(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().is_ring(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().is_ring(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().is_ring(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().is_ring(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().is_ring(),
            Mixed(_, XY) => self.as_mixed::<2>().is_ring(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().is_ring(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().is_ring(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().is_ring(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: NativeArray> IsRing for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_ring(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(chunk.as_ref().is_ring()?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }
}
