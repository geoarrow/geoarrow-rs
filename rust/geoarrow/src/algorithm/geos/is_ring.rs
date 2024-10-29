use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::Result;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::BooleanArray;
use geos::Geom;

/// Returns `true` if the geometry is a ring.
pub trait IsRing {
    type Output;

    fn is_ring(&self) -> Self::Output;
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<const D: usize> IsRing for $type {
            type Output = Result<BooleanArray>;

            fn is_ring(&self) -> Self::Output {
                Ok(self.try_unary_boolean(|geom| geom.to_geos()?.is_ring())?)
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

impl IsRing for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_ring(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().is_ring(),
            LineString(_, XY) => self.as_line_string::<2>().is_ring(),
            Polygon(_, XY) => self.as_polygon::<2>().is_ring(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().is_ring(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().is_ring(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().is_ring(),
            Mixed(_, XY) => self.as_mixed::<2>().is_ring(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().is_ring(),
            Rect(XY) => self.as_rect::<2>().is_ring(),
            Point(_, XYZ) => self.as_point::<3>().is_ring(),
            LineString(_, XYZ) => self.as_line_string::<3>().is_ring(),
            Polygon(_, XYZ) => self.as_polygon::<3>().is_ring(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().is_ring(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().is_ring(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().is_ring(),
            Mixed(_, XYZ) => self.as_mixed::<3>().is_ring(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().is_ring(),
            Rect(XYZ) => self.as_rect::<3>().is_ring(),
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
