use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryScalarTrait;
use crate::NativeArray;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geos::Geom;

/// Returns the length of self. The unit depends of the SRID.
pub trait Length {
    type Output;

    fn length(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Length for PointArray<2> {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Length for $type {
            type Output = Result<Float64Array>;

            fn length(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geos()?.length())?)
            }
        }
    };
}

iter_geos_impl!(LineStringArray<O, 2>);
iter_geos_impl!(MultiPointArray<O, 2>);
iter_geos_impl!(MultiLineStringArray<O, 2>);
iter_geos_impl!(PolygonArray<O, 2>);
iter_geos_impl!(MultiPolygonArray<O, 2>);
iter_geos_impl!(MixedGeometryArray<O, 2>);
iter_geos_impl!(GeometryCollectionArray<O, 2>);
iter_geos_impl!(WKBArray<O>);

impl Length for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().length(),
            LineString(_, XY) => self.as_line_string::<2>().length(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().length(),
            Polygon(_, XY) => self.as_polygon::<2>().length(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().length(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().length(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().length(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().length(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().length(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().length(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().length(),
            Mixed(_, XY) => self.as_mixed::<2>().length(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().length(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().length(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().length(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: NativeArray> Length for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn length(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().length())?.try_into()
    }
}
