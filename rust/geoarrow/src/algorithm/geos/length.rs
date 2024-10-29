use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::Result;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::Float64Array;
use geos::Geom;

/// Returns the length of self. The unit depends of the SRID.
pub trait Length {
    type Output;

    fn length(&self) -> Self::Output;
}

impl<const D: usize> Length for PointArray<D> {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<const D: usize> Length for $type {
            type Output = Result<Float64Array>;

            fn length(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geos()?.length())?)
            }
        }
    };
}

iter_geos_impl!(LineStringArray<D>);
iter_geos_impl!(MultiPointArray<D>);
iter_geos_impl!(MultiLineStringArray<D>);
iter_geos_impl!(PolygonArray<D>);
iter_geos_impl!(MultiPolygonArray<D>);
iter_geos_impl!(MixedGeometryArray<D>);
iter_geos_impl!(GeometryCollectionArray<D>);
iter_geos_impl!(RectArray<D>);

impl Length for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().length(),
            LineString(_, XY) => self.as_line_string::<2>().length(),
            Polygon(_, XY) => self.as_polygon::<2>().length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().length(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().length(),
            Mixed(_, XY) => self.as_mixed::<2>().length(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().length(),
            Rect(XY) => self.as_rect::<2>().length(),
            Point(_, XYZ) => self.as_point::<3>().length(),
            LineString(_, XYZ) => self.as_line_string::<3>().length(),
            Polygon(_, XYZ) => self.as_polygon::<3>().length(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().length(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().length(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().length(),
            Mixed(_, XYZ) => self.as_mixed::<3>().length(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().length(),
            Rect(XYZ) => self.as_rect::<3>().length(),
        }
    }
}

impl<G: NativeArray> Length for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn length(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().length())?.try_into()
    }
}
