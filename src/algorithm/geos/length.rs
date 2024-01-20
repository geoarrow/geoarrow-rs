use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geos::Geom;

/// Returns the length of self. The unit depends of the SRID.
pub trait Length {
    type Output;

    fn length(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Length for PointArray {
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

iter_geos_impl!(LineStringArray<O>);
iter_geos_impl!(MultiPointArray<O>);
iter_geos_impl!(MultiLineStringArray<O>);
iter_geos_impl!(PolygonArray<O>);
iter_geos_impl!(MultiPolygonArray<O>);
iter_geos_impl!(MixedGeometryArray<O>);
iter_geos_impl!(GeometryCollectionArray<O>);
iter_geos_impl!(WKBArray<O>);

impl Length for &dyn GeometryArrayTrait {
    type Output = Result<Float64Array>;

    fn length(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().length(),
            GeoDataType::LineString(_) => self.as_line_string().length(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().length(),
            GeoDataType::Polygon(_) => self.as_polygon().length(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().length(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().length(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().length(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().length(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().length(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().length(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().length(),
            GeoDataType::Mixed(_) => self.as_mixed().length(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().length(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().length(),
            GeoDataType::LargeGeometryCollection(_) => self.as_large_geometry_collection().length(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: GeometryArrayTrait> Length for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn length(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().length())?.try_into()
    }
}
