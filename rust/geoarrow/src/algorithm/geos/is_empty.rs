use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, NativeType};
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
        impl<const D: usize> IsEmpty for $type {
            type Output = Result<BooleanArray>;

            fn is_empty(&self) -> Self::Output {
                Ok(self.try_unary_boolean(|geom| geom.to_geos()?.is_empty())?)
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

impl IsEmpty for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_empty(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => IsEmpty::is_empty(self.as_point::<2>()),
            LineString(_, XY) => IsEmpty::is_empty(self.as_line_string::<2>()),
            Polygon(_, XY) => IsEmpty::is_empty(self.as_polygon::<2>()),
            MultiPoint(_, XY) => IsEmpty::is_empty(self.as_multi_point::<2>()),
            MultiLineString(_, XY) => IsEmpty::is_empty(self.as_multi_line_string::<2>()),
            MultiPolygon(_, XY) => IsEmpty::is_empty(self.as_multi_polygon::<2>()),
            Mixed(_, XY) => IsEmpty::is_empty(self.as_mixed::<2>()),
            GeometryCollection(_, XY) => IsEmpty::is_empty(self.as_geometry_collection::<2>()),
            Rect(XY) => IsEmpty::is_empty(self.as_rect::<2>()),
            Point(_, XYZ) => IsEmpty::is_empty(self.as_point::<3>()),
            LineString(_, XYZ) => IsEmpty::is_empty(self.as_line_string::<3>()),
            Polygon(_, XYZ) => IsEmpty::is_empty(self.as_polygon::<3>()),
            MultiPoint(_, XYZ) => IsEmpty::is_empty(self.as_multi_point::<3>()),
            MultiLineString(_, XYZ) => IsEmpty::is_empty(self.as_multi_line_string::<3>()),
            MultiPolygon(_, XYZ) => IsEmpty::is_empty(self.as_multi_polygon::<3>()),
            Mixed(_, XYZ) => IsEmpty::is_empty(self.as_mixed::<3>()),
            GeometryCollection(_, XYZ) => IsEmpty::is_empty(self.as_geometry_collection::<3>()),
            Rect(XYZ) => IsEmpty::is_empty(self.as_rect::<3>()),
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
