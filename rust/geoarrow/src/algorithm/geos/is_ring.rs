use crate::NativeArray;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::NativeScalar;
use arrow_array::BooleanArray;
use geos::Geom;

/// Returns `true` if the geometry is a ring.
pub trait IsRing {
    type Output;

    fn is_ring(&self) -> Self::Output;
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl IsRing for $type {
            type Output = Result<BooleanArray>;

            fn is_ring(&self) -> Self::Output {
                Ok(self.try_unary_boolean(|geom| geom.to_geos()?.is_ring())?)
            }
        }
    };
}

iter_geos_impl!(PointArray);
iter_geos_impl!(LineStringArray);
iter_geos_impl!(MultiPointArray);
iter_geos_impl!(MultiLineStringArray);
iter_geos_impl!(PolygonArray);
iter_geos_impl!(MultiPolygonArray);
iter_geos_impl!(MixedGeometryArray);
iter_geos_impl!(GeometryCollectionArray);
iter_geos_impl!(RectArray);
iter_geos_impl!(GeometryArray);

impl IsRing for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_ring(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().is_ring(),
            LineString(_) => self.as_line_string().is_ring(),
            Polygon(_) => self.as_polygon().is_ring(),
            MultiPoint(_) => self.as_multi_point().is_ring(),
            MultiLineString(_) => self.as_multi_line_string().is_ring(),
            MultiPolygon(_) => self.as_multi_polygon().is_ring(),
            GeometryCollection(_) => self.as_geometry_collection().is_ring(),
            Rect(_) => self.as_rect().is_ring(),
            Geometry(_) => self.as_geometry().is_ring(),
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
