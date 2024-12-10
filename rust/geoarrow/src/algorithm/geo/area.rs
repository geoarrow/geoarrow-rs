use crate::algorithm::geo::utils::zeroes;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::Float64Array;
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
/// use geoarrow::datatypes::Dimension;
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
/// let polygon_array: PolygonArray = (vec![polygon].as_slice(), Dimension::XY).into();
/// let reversed_polygon_array: PolygonArray = (vec![reversed_polygon].as_slice(), Dimension::XY).into();
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
}

zero_impl!(PointArray);
zero_impl!(LineStringArray);
zero_impl!(MultiPointArray);
zero_impl!(MultiLineStringArray);

macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl Area for $type {
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

iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);
iter_geo_impl!(RectArray);
iter_geo_impl!(GeometryArray);

impl Area for &dyn NativeArray {
    type Output = Result<Float64Array>;

    fn signed_area(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, _) => self.as_point().signed_area(),
            LineString(_, _) => self.as_line_string().signed_area(),
            Polygon(_, _) => self.as_polygon().signed_area(),
            MultiPoint(_, _) => self.as_multi_point().signed_area(),
            MultiLineString(_, _) => self.as_multi_line_string().signed_area(),
            MultiPolygon(_, _) => self.as_multi_polygon().signed_area(),
            Mixed(_, _) => self.as_mixed().signed_area(),
            GeometryCollection(_, _) => self.as_geometry_collection().signed_area(),
            Rect(_) => self.as_rect().signed_area(),
            Geometry(_) => self.as_geometry().signed_area(),
        };
        Ok(result)
    }

    fn unsigned_area(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, _) => self.as_point().unsigned_area(),
            LineString(_, _) => self.as_line_string().unsigned_area(),
            Polygon(_, _) => self.as_polygon().unsigned_area(),
            MultiPoint(_, _) => self.as_multi_point().unsigned_area(),
            MultiLineString(_, _) => self.as_multi_line_string().unsigned_area(),
            MultiPolygon(_, _) => self.as_multi_polygon().unsigned_area(),
            Mixed(_, _) => self.as_mixed().unsigned_area(),
            GeometryCollection(_, _) => self.as_geometry_collection().unsigned_area(),
            Rect(_) => self.as_rect().unsigned_area(),
            Geometry(_) => self.as_geometry().unsigned_area(),
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
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().signed_area(),
            LineString(_, _) => self.as_line_string().signed_area(),
            Polygon(_, _) => self.as_polygon().signed_area(),
            MultiPoint(_, _) => self.as_multi_point().signed_area(),
            MultiLineString(_, _) => self.as_multi_line_string().signed_area(),
            MultiPolygon(_, _) => self.as_multi_polygon().signed_area(),
            Mixed(_, _) => self.as_mixed().signed_area(),
            GeometryCollection(_, _) => self.as_geometry_collection().signed_area(),
            Rect(_) => self.as_rect().signed_area(),
            Geometry(_) => self.as_geometry().unsigned_area(),
        }
    }

    fn unsigned_area(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_, _) => self.as_point().unsigned_area(),
            LineString(_, _) => self.as_line_string().unsigned_area(),
            Polygon(_, _) => self.as_polygon().unsigned_area(),
            MultiPoint(_, _) => self.as_multi_point().unsigned_area(),
            MultiLineString(_, _) => self.as_multi_line_string().unsigned_area(),
            MultiPolygon(_, _) => self.as_multi_polygon().unsigned_area(),
            Mixed(_, _) => self.as_mixed().unsigned_area(),
            GeometryCollection(_, _) => self.as_geometry_collection().unsigned_area(),
            Rect(_) => self.as_rect().unsigned_area(),
            Geometry(_) => self.as_geometry().unsigned_area(),
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
