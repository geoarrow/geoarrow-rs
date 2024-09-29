use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use crate::NativeArray;
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
        impl IsValid for $type {
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

iter_geos_impl!(LineStringArray<2>);
iter_geos_impl!(MultiPointArray<2>);
iter_geos_impl!(MultiLineStringArray<2>);
iter_geos_impl!(PolygonArray<2>);
iter_geos_impl!(MultiPolygonArray<2>);
iter_geos_impl!(MixedGeometryArray<2>);
iter_geos_impl!(GeometryCollectionArray<2>);

impl IsValid for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_valid(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => IsValid::is_valid(self.as_point::<2>()),
            LineString(_, XY) => IsValid::is_valid(self.as_line_string::<2>()),
            Polygon(_, XY) => IsValid::is_valid(self.as_polygon::<2>()),
            MultiPoint(_, XY) => IsValid::is_valid(self.as_multi_point::<2>()),
            MultiLineString(_, XY) => IsValid::is_valid(self.as_multi_line_string::<2>()),
            MultiPolygon(_, XY) => IsValid::is_valid(self.as_multi_polygon::<2>()),
            Mixed(_, XY) => IsValid::is_valid(self.as_mixed::<2>()),
            GeometryCollection(_, XY) => IsValid::is_valid(self.as_geometry_collection::<2>()),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: NativeArray> IsValid for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_valid(&self) -> Self::Output {
        let mut output_chunks = Vec::with_capacity(self.chunks.len());
        for chunk in self.chunks.iter() {
            output_chunks.push(IsValid::is_valid(&chunk.as_ref())?);
        }

        Ok(ChunkedArray::new(output_chunks))
    }
}
