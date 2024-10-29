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
pub trait IsSimple {
    type Output;

    fn is_simple(&self) -> Self::Output;
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<const D: usize> IsSimple for $type {
            type Output = Result<BooleanArray>;

            fn is_simple(&self) -> Self::Output {
                Ok(self.try_unary_boolean(|geom| geom.to_geos()?.is_simple())?)
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

impl IsSimple for &dyn NativeArray {
    type Output = Result<BooleanArray>;

    fn is_simple(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().is_simple(),
            LineString(_, XY) => self.as_line_string::<2>().is_simple(),
            Polygon(_, XY) => self.as_polygon::<2>().is_simple(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().is_simple(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().is_simple(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().is_simple(),
            Mixed(_, XY) => self.as_mixed::<2>().is_simple(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().is_simple(),
            Rect(XY) => self.as_rect::<2>().is_simple(),
            Point(_, XYZ) => self.as_point::<3>().is_simple(),
            LineString(_, XYZ) => self.as_line_string::<3>().is_simple(),
            Polygon(_, XYZ) => self.as_polygon::<3>().is_simple(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().is_simple(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().is_simple(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().is_simple(),
            Mixed(_, XYZ) => self.as_mixed::<3>().is_simple(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().is_simple(),
            Rect(XYZ) => self.as_rect::<3>().is_simple(),
        }
    }
}

impl<G: NativeArray> IsSimple for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<BooleanArray>>;

    fn is_simple(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().is_simple())?.try_into()
    }
}
