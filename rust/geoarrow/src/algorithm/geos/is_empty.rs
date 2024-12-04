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
pub trait IsEmpty {
    type Output;

    fn is_empty(&self) -> Self::Output;
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl IsEmpty for $type {
            type Output = Result<BooleanArray>;

            fn is_empty(&self) -> Self::Output {
                Ok(self.try_unary_boolean(|geom| geom.to_geos()?.is_empty())?)
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
iter_geos_impl!(UnknownGeometryArray);

impl IsEmpty for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => IsEmpty::is_empty(self.as_point()),
            LineString(_, _) => IsEmpty::is_empty(self.as_line_string()),
            Polygon(_, _) => IsEmpty::is_empty(self.as_polygon()),
            MultiPoint(_, _) => IsEmpty::is_empty(self.as_multi_point()),
            MultiLineString(_, _) => IsEmpty::is_empty(self.as_multi_line_string()),
            MultiPolygon(_, _) => IsEmpty::is_empty(self.as_multi_polygon()),
            Mixed(_, _) => IsEmpty::is_empty(self.as_mixed()),
            GeometryCollection(_, _) => IsEmpty::is_empty(self.as_geometry_collection()),
            Rect(_) => IsEmpty::is_empty(self.as_rect()),
            Unknown(_) => IsEmpty::is_empty(self.as_unknown()),
        }
    }
}

impl<G: NativeArray> IsEmpty for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_empty(&self) -> Self::Output {
        self.try_map(|chunk| IsEmpty::is_empty(&chunk.as_ref()))?
            .try_into()
    }
}
