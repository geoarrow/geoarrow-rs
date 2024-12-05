use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::NativeType;
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

impl Length for PointArray {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl Length for $type {
            type Output = Result<Float64Array>;

            fn length(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geos()?.length())?)
            }
        }
    };
}

iter_geos_impl!(LineStringArray);
iter_geos_impl!(MultiPointArray);
iter_geos_impl!(MultiLineStringArray);
iter_geos_impl!(PolygonArray);
iter_geos_impl!(MultiPolygonArray);
iter_geos_impl!(MixedGeometryArray);
iter_geos_impl!(GeometryCollectionArray);
iter_geos_impl!(RectArray);
iter_geos_impl!(GeometryArray);

impl Length for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().length(),
            LineString(_, _) => self.as_line_string().length(),
            Polygon(_, _) => self.as_polygon().length(),
            MultiPoint(_, _) => self.as_multi_point().length(),
            MultiLineString(_, _) => self.as_multi_line_string().length(),
            MultiPolygon(_, _) => self.as_multi_polygon().length(),
            GeometryCollection(_, _) => self.as_geometry_collection().length(),
            Rect(_) => self.as_rect().length(),
            Geometry(_) => self.as_geometry().length(),
        }
    }
}

impl<G: NativeArray> Length for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn length(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().length())?.try_into()
    }
}
