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

/// Unsigned planar area of a geometry.
pub trait Area {
    type Output;

    fn area(&self) -> Self::Output;
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl<const D: usize> Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(zeroes(self.len(), self.nulls()))
            }
        }
    };
}

zero_impl!(PointArray<D>);
zero_impl!(LineStringArray<D>);
zero_impl!(MultiPointArray<D>);
zero_impl!(MultiLineStringArray<D>);

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl<const D: usize> Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geos()?.area())?)
            }
        }
    };
}

iter_geos_impl!(PolygonArray<D>);
iter_geos_impl!(MultiPolygonArray<D>);
iter_geos_impl!(MixedGeometryArray<D>);
iter_geos_impl!(GeometryCollectionArray<D>);
iter_geos_impl!(RectArray<D>);

impl Area for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().area(),
            LineString(_, XY) => self.as_line_string::<2>().area(),
            Polygon(_, XY) => self.as_polygon::<2>().area(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().area(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().area(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().area(),
            Mixed(_, XY) => self.as_mixed::<2>().area(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().area(),
            Rect(XY) => self.as_rect::<2>().area(),
            Point(_, XYZ) => self.as_point::<3>().area(),
            LineString(_, XYZ) => self.as_line_string::<3>().area(),
            Polygon(_, XYZ) => self.as_polygon::<3>().area(),
            MultiPoint(_, XYZ) => self.as_multi_point::<3>().area(),
            MultiLineString(_, XYZ) => self.as_multi_line_string::<3>().area(),
            MultiPolygon(_, XYZ) => self.as_multi_polygon::<3>().area(),
            Mixed(_, XYZ) => self.as_mixed::<3>().area(),
            GeometryCollection(_, XYZ) => self.as_geometry_collection::<3>().area(),
            Rect(XYZ) => self.as_rect::<3>().area(),
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
