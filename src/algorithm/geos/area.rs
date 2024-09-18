use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geos::Geom;

/// Unsigned planar area of a geometry.
pub trait Area {
    type Output;

    fn area(&self) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Area for PointArray<2> {
    type Output = Result<Float64Array>;

    fn area(&self) -> Self::Output {
        Ok(zeroes(self.len(), self.nulls()))
    }
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(zeroes(self.len(), self.nulls()))
            }
        }
    };
}

zero_impl!(LineStringArray<O, 2>);
zero_impl!(MultiPointArray<O, 2>);
zero_impl!(MultiLineStringArray<O, 2>);

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geos()?.area())?)
            }
        }
    };
}

iter_geos_impl!(PolygonArray<O, 2>);
iter_geos_impl!(MultiPolygonArray<O, 2>);
iter_geos_impl!(MixedGeometryArray<O, 2>);
iter_geos_impl!(GeometryCollectionArray<O, 2>);
iter_geos_impl!(WKBArray<O>);

impl Area for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().area(),
            LineString(_, XY) => self.as_line_string::<2>().area(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().area(),
            Polygon(_, XY) => self.as_polygon::<2>().area(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().area(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().area(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().area(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().area(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().area(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().area(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().area(),
            Mixed(_, XY) => self.as_mixed::<2>().area(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().area(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().area(),
            LargeGeometryCollection(_, XY) => self.as_large_geometry_collection::<2>().area(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

impl<G: NativeArray> Area for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn area(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().area())?.try_into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn tmp() {
        let arr = p_array();
        let area = arr.area().unwrap();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
