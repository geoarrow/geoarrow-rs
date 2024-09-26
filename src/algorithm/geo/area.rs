use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::NativeScalar;
// use crate::array::ArrayBase;
use crate::NativeArray;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::prelude::Area as GeoArea;

/// Signed and unsigned planar area of a geometry.
///
/// # Examples
///
/// ```
/// use geo::polygon;
///
/// use geoarrow::algorithm::geo::Area;
/// use geoarrow::array::PolygonArray;
///
/// let polygon = polygon![
///     (x: 0., y: 0.),
///     (x: 5., y: 0.),
///     (x: 5., y: 6.),
///     (x: 0., y: 6.),
///     (x: 0., y: 0.),
/// ];
///
/// let mut reversed_polygon = polygon.clone();
/// reversed_polygon.exterior_mut(|line_string| {
///     line_string.0.reverse();
/// });
///
/// let polygon_array: PolygonArray<i32, 2> = vec![polygon].as_slice().into();
/// let reversed_polygon_array: PolygonArray<i32, 2> = vec![reversed_polygon].as_slice().into();
///
/// assert_eq!(polygon_array.signed_area().value(0), 30.);
/// assert_eq!(polygon_array.unsigned_area().value(0), 30.);
///
/// assert_eq!(reversed_polygon_array.signed_area().value(0), -30.);
/// assert_eq!(reversed_polygon_array.unsigned_area().value(0), 30.);
/// ```
pub trait Area {
    type Output;

    fn signed_area(&self) -> Self::Output;

    fn unsigned_area(&self) -> Self::Output;
}

/// Implementation where the result is zero.
macro_rules! zero_impl {
    ($type:ty) => {
        impl Area for $type {
            type Output = Float64Array;

            fn signed_area(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }

            fn unsigned_area(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
    ($type:ty, "O") => {
        impl<O: OffsetSizeTrait> Area for $type {
            type Output = Float64Array;

            fn signed_area(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }

            fn unsigned_area(&self) -> Self::Output {
                zeroes(self.len(), self.nulls())
            }
        }
    };
}

zero_impl!(PointArray<2>);
zero_impl!(LineStringArray<O, 2>, "O");
zero_impl!(MultiPointArray<O, 2>, "O");
zero_impl!(MultiLineStringArray<O, 2>, "O");

macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Area for $type {
            type Output = Float64Array;

            fn signed_area(&self) -> Self::Output {
                self.unary_primitive(|geom| geom.to_geo().signed_area())
            }

            fn unsigned_area(&self) -> Self::Output {
                self.unary_primitive(|geom| geom.to_geo().unsigned_area())
            }
        }
    };
}

iter_geo_impl!(PolygonArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>);
iter_geo_impl!(MixedGeometryArray<O, 2>);
iter_geo_impl!(GeometryCollectionArray<O, 2>);
iter_geo_impl!(WKBArray<O>);

impl Area for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn signed_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().signed_area(),
            LineString(_, XY) => self.as_line_string::<2>().signed_area(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().signed_area(),
            Polygon(_, XY) => self.as_polygon::<2>().signed_area(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().signed_area(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().signed_area(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().signed_area(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().signed_area(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().signed_area(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().signed_area(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().signed_area(),
            Mixed(_, XY) => self.as_mixed::<2>().signed_area(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().signed_area(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().signed_area(),
            LargeGeometryCollection(_, XY) => {
                self.as_large_geometry_collection::<2>().signed_area()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }

    fn unsigned_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().unsigned_area(),
            LineString(_, XY) => self.as_line_string::<2>().unsigned_area(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().unsigned_area(),
            Polygon(_, XY) => self.as_polygon::<2>().unsigned_area(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().unsigned_area(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().unsigned_area(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().unsigned_area(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().unsigned_area(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().unsigned_area(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().unsigned_area(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().unsigned_area(),
            Mixed(_, XY) => self.as_mixed::<2>().unsigned_area(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().unsigned_area(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().unsigned_area(),
            LargeGeometryCollection(_, XY) => {
                self.as_large_geometry_collection::<2>().unsigned_area()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> Area for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn signed_area(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().signed_area())?
            .try_into()
    }

    fn unsigned_area(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().unsigned_area())?
            .try_into()
    }
}

impl Area for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedArray<Float64Array>>;

    fn signed_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().signed_area(),
            LineString(_, XY) => self.as_line_string::<2>().signed_area(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().signed_area(),
            Polygon(_, XY) => self.as_polygon::<2>().signed_area(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().signed_area(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().signed_area(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().signed_area(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().signed_area(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().signed_area(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().signed_area(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().signed_area(),
            Mixed(_, XY) => self.as_mixed::<2>().signed_area(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().signed_area(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().signed_area(),
            LargeGeometryCollection(_, XY) => {
                self.as_large_geometry_collection::<2>().signed_area()
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }

    fn unsigned_area(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().unsigned_area(),
            LineString(_, XY) => self.as_line_string::<2>().unsigned_area(),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().unsigned_area(),
            Polygon(_, XY) => self.as_polygon::<2>().unsigned_area(),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().unsigned_area(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().unsigned_area(),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().unsigned_area(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().unsigned_area(),
            LargeMultiLineString(_, XY) => self.as_large_multi_line_string::<2>().unsigned_area(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().unsigned_area(),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().unsigned_area(),
            Mixed(_, XY) => self.as_mixed::<2>().unsigned_area(),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().unsigned_area(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().unsigned_area(),
            LargeGeometryCollection(_, XY) => {
                self.as_large_geometry_collection::<2>().unsigned_area()
            }
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn area() {
        let arr = p_array();
        let area = arr.unsigned_area();
        assert_eq!(area, Float64Array::new(vec![28., 18.].into(), None));
    }
}
