use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPointArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::BoundingRect;
use geoarrow_schema::Dimension;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of that box
pub trait Center {
    type Output;

    fn center(&self) -> Self::Output;
}

impl Center for PointArray {
    type Output = PointArray;

    fn center(&self) -> Self::Output {
        self.clone()
    }
}

impl Center for RectArray {
    type Output = PointArray;

    fn center(&self) -> Self::Output {
        let mut output_array = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            self.len(),
            self.coord_type(),
            self.metadata().clone(),
        );
        self.iter_geo().for_each(|maybe_g| {
            output_array.push_coord(maybe_g.map(|g| g.bounding_rect().center()).as_ref())
        });
        output_array.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl Center for $type {
            type Output = PointArray;

            fn center(&self) -> Self::Output {
                let mut output_array = PointBuilder::with_capacity_and_options(
                    Dimension::XY,
                    self.len(),
                    self.coord_type(),
                    self.metadata().clone(),
                );
                self.iter_geo().for_each(|maybe_g| {
                    output_array.push_coord(
                        maybe_g
                            .and_then(|g| g.bounding_rect().map(|rect| rect.center()))
                            .as_ref(),
                    )
                });
                output_array.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);
iter_geo_impl!(GeometryArray);

impl Center for &dyn NativeArray {
    type Output = Result<PointArray>;

    fn center(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => self.as_point().center(),
            LineString(_) => self.as_line_string().center(),
            Polygon(_) => self.as_polygon().center(),
            MultiPoint(_) => self.as_multi_point().center(),
            MultiLineString(_) => self.as_multi_line_string().center(),
            MultiPolygon(_) => self.as_multi_polygon().center(),
            GeometryCollection(_) => self.as_geometry_collection().center(),
            Rect(_) => self.as_rect().center(),
            Geometry(_) => self.as_geometry().center(),
        };
        Ok(result)
    }
}

impl<G: NativeArray> Center for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedPointArray>;

    fn center(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().center())?.try_into()
    }
}

impl Center for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPointArray>;

    fn center(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().center(),
            LineString(_) => self.as_line_string().center(),
            Polygon(_) => self.as_polygon().center(),
            MultiPoint(_) => self.as_multi_point().center(),
            MultiLineString(_) => self.as_multi_line_string().center(),
            MultiPolygon(_) => self.as_multi_polygon().center(),
            GeometryCollection(_) => self.as_geometry_collection().center(),
            Rect(_) => self.as_rect().center(),
            Geometry(_) => self.as_geometry().center(),
        }
    }
}
