use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::Result;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::BooleanArray;
use geos::Geom;

/// Checks if the geometry is valid
pub trait IsValid {
    type Output;

    fn is_valid(&self) -> Self::Output;
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<const D: usize> IsValid for $type {
            type Output = Result<BooleanArray>;

            fn is_valid(&self) -> Self::Output {
                Ok(self
                    .try_unary_boolean(|geom| Ok::<_, geos::Error>(geom.to_geos()?.is_valid()))?)
            }
        }
    };
}

iter_geos_impl!(PointArray<D>);
iter_geos_impl!(LineStringArray<D>);
iter_geos_impl!(MultiPointArray<D>);
iter_geos_impl!(MultiLineStringArray<D>);
iter_geos_impl!(PolygonArray<D>);
iter_geos_impl!(MultiPolygonArray<D>);
iter_geos_impl!(MixedGeometryArray<D>);
iter_geos_impl!(GeometryCollectionArray<D>);
iter_geos_impl!(RectArray<D>);

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
            Rect(XY) => IsValid::is_valid(self.as_rect::<2>()),
            Point(_, XYZ) => IsValid::is_valid(self.as_point::<3>()),
            LineString(_, XYZ) => IsValid::is_valid(self.as_line_string::<3>()),
            Polygon(_, XYZ) => IsValid::is_valid(self.as_polygon::<3>()),
            MultiPoint(_, XYZ) => IsValid::is_valid(self.as_multi_point::<3>()),
            MultiLineString(_, XYZ) => IsValid::is_valid(self.as_multi_line_string::<3>()),
            MultiPolygon(_, XYZ) => IsValid::is_valid(self.as_multi_polygon::<3>()),
            Mixed(_, XYZ) => IsValid::is_valid(self.as_mixed::<3>()),
            GeometryCollection(_, XYZ) => IsValid::is_valid(self.as_geometry_collection::<3>()),
            Rect(XYZ) => IsValid::is_valid(self.as_rect::<3>()),
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
