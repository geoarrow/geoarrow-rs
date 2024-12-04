use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::BooleanArray;
use geos::Geom;

/// Returns `true` if the geometry is empty.
pub trait IsSimple {
    type Output;

    fn is_simple(&self) -> Self::Output;
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl IsSimple for $type {
            type Output = Result<BooleanArray>;

            fn is_simple(&self) -> Self::Output {
                Ok(self.try_unary_boolean(|geom| geom.to_geos()?.is_simple())?)
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

impl IsSimple for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_simple(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().is_simple(),
            LineString(_, _) => self.as_line_string().is_simple(),
            Polygon(_, _) => self.as_polygon().is_simple(),
            MultiPoint(_, _) => self.as_multi_point().is_simple(),
            MultiLineString(_, _) => self.as_multi_line_string().is_simple(),
            MultiPolygon(_, _) => self.as_multi_polygon().is_simple(),
            Mixed(_, _) => self.as_mixed().is_simple(),
            GeometryCollection(_, _) => self.as_geometry_collection().is_simple(),
            Rect(_) => self.as_rect().is_simple(),
            Geometry(_) => self.as_geometry().is_simple(),
        }
    }
}

impl<G: NativeArray> IsSimple for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_simple(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().is_simple())?.try_into()
    }
}
