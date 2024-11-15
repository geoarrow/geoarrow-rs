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

/// Unsigned planar area of a geometry.
pub trait Area {
    type Output;

    fn area(&self) -> Self::Output;
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(zeroes(self.len(), self.nulls()))
            }
        }
    };
}

zero_impl!(PointArray);
zero_impl!(LineStringArray);
zero_impl!(MultiPointArray);
zero_impl!(MultiLineStringArray);

macro_rules! iter_geos_impl {
    ($type:ty) => {
        impl Area for $type {
            type Output = Result<Float64Array>;

            fn area(&self) -> Self::Output {
                Ok(self.try_unary_primitive(|geom| geom.to_geos()?.area())?)
            }
        }
    };
}

iter_geos_impl!(PolygonArray);
iter_geos_impl!(MultiPolygonArray);
iter_geos_impl!(MixedGeometryArray);
iter_geos_impl!(GeometryCollectionArray);
iter_geos_impl!(RectArray);

impl Area for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn area(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().area(),
            LineString(_, _) => self.as_line_string().area(),
            Polygon(_, _) => self.as_polygon().area(),
            MultiPoint(_, _) => self.as_multi_point().area(),
            MultiLineString(_, _) => self.as_multi_line_string().area(),
            MultiPolygon(_, _) => self.as_multi_polygon().area(),
            Mixed(_, _) => self.as_mixed().area(),
            GeometryCollection(_, _) => self.as_geometry_collection().area(),
            Rect(_) => self.as_rect().area(),
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
