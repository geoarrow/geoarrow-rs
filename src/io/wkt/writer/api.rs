use arrow::array::GenericStringBuilder;
use arrow_array::OffsetSizeTrait;

use crate::array::{AsChunkedNativeArray, AsNativeArray, WKTArray};
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::io::wkt::writer::scalar::{
    geometry_collection_to_wkt, geometry_to_wkt, line_string_to_wkt, multi_line_string_to_wkt,
    multi_point_to_wkt, multi_polygon_to_wkt, point_to_wkt, polygon_to_wkt, rect_to_wkt,
};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;

pub trait ToWKT {
    type Output<O: OffsetSizeTrait>;

    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O>;
}

impl ToWKT for &dyn NativeArray {
    type Output<O: OffsetSizeTrait> = WKTArray<O>;

    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        let metadata = self.metadata();
        let mut output_array = GenericStringBuilder::<O>::new();

        use Dimension::*;
        use NativeType::*;

        macro_rules! impl_to_wkt {
            ($cast_func:ident, $dim:expr, $to_wkt_func:expr) => {
                for maybe_geom in self.$cast_func::<$dim>().iter() {
                    output_array
                        .append_option(maybe_geom.map(|geom| $to_wkt_func(&geom).to_string()));
                }
            };
        }

        match self.data_type() {
            Point(_, XY) => impl_to_wkt!(as_point, 2, point_to_wkt),
            LineString(_, XY) => impl_to_wkt!(as_line_string, 2, line_string_to_wkt),
            LargeLineString(_, XY) => impl_to_wkt!(as_large_line_string, 2, line_string_to_wkt),
            Polygon(_, XY) => impl_to_wkt!(as_polygon, 2, polygon_to_wkt),
            LargePolygon(_, XY) => impl_to_wkt!(as_large_polygon, 2, polygon_to_wkt),
            MultiPoint(_, XY) => impl_to_wkt!(as_multi_point, 2, multi_point_to_wkt),
            LargeMultiPoint(_, XY) => impl_to_wkt!(as_large_multi_point, 2, multi_point_to_wkt),
            MultiLineString(_, XY) => {
                impl_to_wkt!(as_multi_line_string, 2, multi_line_string_to_wkt)
            }
            LargeMultiLineString(_, XY) => {
                impl_to_wkt!(as_large_multi_line_string, 2, multi_line_string_to_wkt)
            }
            MultiPolygon(_, XY) => impl_to_wkt!(as_multi_polygon, 2, multi_polygon_to_wkt),
            LargeMultiPolygon(_, XY) => {
                impl_to_wkt!(as_large_multi_polygon, 2, multi_polygon_to_wkt)
            }
            Mixed(_, XY) => impl_to_wkt!(as_mixed, 2, geometry_to_wkt),
            LargeMixed(_, XY) => impl_to_wkt!(as_large_mixed, 2, geometry_to_wkt),
            GeometryCollection(_, XY) => {
                impl_to_wkt!(as_geometry_collection, 2, geometry_collection_to_wkt)
            }
            LargeGeometryCollection(_, XY) => {
                impl_to_wkt!(as_large_geometry_collection, 2, geometry_collection_to_wkt)
            }
            Point(_, XYZ) => impl_to_wkt!(as_point, 3, point_to_wkt),
            LineString(_, XYZ) => impl_to_wkt!(as_line_string, 3, line_string_to_wkt),
            LargeLineString(_, XYZ) => impl_to_wkt!(as_large_line_string, 3, line_string_to_wkt),
            Polygon(_, XYZ) => impl_to_wkt!(as_polygon, 3, polygon_to_wkt),
            LargePolygon(_, XYZ) => impl_to_wkt!(as_large_polygon, 3, polygon_to_wkt),
            MultiPoint(_, XYZ) => impl_to_wkt!(as_multi_point, 3, multi_point_to_wkt),
            LargeMultiPoint(_, XYZ) => impl_to_wkt!(as_large_multi_point, 3, multi_point_to_wkt),
            MultiLineString(_, XYZ) => {
                impl_to_wkt!(as_multi_line_string, 3, multi_line_string_to_wkt)
            }
            LargeMultiLineString(_, XYZ) => {
                impl_to_wkt!(as_large_multi_line_string, 3, multi_line_string_to_wkt)
            }
            MultiPolygon(_, XYZ) => impl_to_wkt!(as_multi_polygon, 3, multi_polygon_to_wkt),
            LargeMultiPolygon(_, XYZ) => {
                impl_to_wkt!(as_large_multi_polygon, 3, multi_polygon_to_wkt)
            }
            Mixed(_, XYZ) => impl_to_wkt!(as_mixed, 3, geometry_to_wkt),
            LargeMixed(_, XYZ) => impl_to_wkt!(as_large_mixed, 3, geometry_to_wkt),
            GeometryCollection(_, XYZ) => {
                impl_to_wkt!(as_geometry_collection, 3, geometry_collection_to_wkt)
            }
            LargeGeometryCollection(_, XYZ) => {
                impl_to_wkt!(as_large_geometry_collection, 3, geometry_collection_to_wkt)
            }
            Rect(XY) => impl_to_wkt!(as_rect, 2, rect_to_wkt),
            Rect(XYZ) => impl_to_wkt!(as_rect, 3, rect_to_wkt),
        }

        WKTArray::new(output_array.finish(), metadata)
    }
}

impl ToWKT for &dyn ChunkedNativeArray {
    type Output<O: OffsetSizeTrait> = ChunkedGeometryArray<WKTArray<O>>;

    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use Dimension::*;
        use NativeType::*;

        macro_rules! impl_to_wkt {
            ($cast_func:ident, $dim:expr) => {
                ChunkedGeometryArray::new(
                    self.$cast_func::<$dim>()
                        .map(|chunk| chunk.as_ref().to_wkt()),
                )
            };
        }

        match self.data_type() {
            Point(_, XY) => impl_to_wkt!(as_point, 2),
            LineString(_, XY) => impl_to_wkt!(as_line_string, 2),
            LargeLineString(_, XY) => impl_to_wkt!(as_large_line_string, 2),
            Polygon(_, XY) => impl_to_wkt!(as_polygon, 2),
            LargePolygon(_, XY) => impl_to_wkt!(as_large_polygon, 2),
            MultiPoint(_, XY) => impl_to_wkt!(as_multi_point, 2),
            LargeMultiPoint(_, XY) => impl_to_wkt!(as_large_multi_point, 2),
            MultiLineString(_, XY) => impl_to_wkt!(as_multi_line_string, 2),
            LargeMultiLineString(_, XY) => impl_to_wkt!(as_large_multi_line_string, 2),
            MultiPolygon(_, XY) => impl_to_wkt!(as_multi_polygon, 2),
            LargeMultiPolygon(_, XY) => impl_to_wkt!(as_large_multi_polygon, 2),
            Mixed(_, XY) => impl_to_wkt!(as_mixed, 2),
            LargeMixed(_, XY) => impl_to_wkt!(as_large_mixed, 2),
            GeometryCollection(_, XY) => impl_to_wkt!(as_geometry_collection, 2),
            LargeGeometryCollection(_, XY) => impl_to_wkt!(as_large_geometry_collection, 2),
            Rect(XY) => impl_to_wkt!(as_rect, 2),

            Point(_, XYZ) => impl_to_wkt!(as_point, 3),
            LineString(_, XYZ) => impl_to_wkt!(as_line_string, 3),
            LargeLineString(_, XYZ) => impl_to_wkt!(as_large_line_string, 3),
            Polygon(_, XYZ) => impl_to_wkt!(as_polygon, 3),
            LargePolygon(_, XYZ) => impl_to_wkt!(as_large_polygon, 3),
            MultiPoint(_, XYZ) => impl_to_wkt!(as_multi_point, 3),
            LargeMultiPoint(_, XYZ) => impl_to_wkt!(as_large_multi_point, 3),
            MultiLineString(_, XYZ) => impl_to_wkt!(as_multi_line_string, 3),
            LargeMultiLineString(_, XYZ) => impl_to_wkt!(as_large_multi_line_string, 3),
            MultiPolygon(_, XYZ) => impl_to_wkt!(as_multi_polygon, 3),
            LargeMultiPolygon(_, XYZ) => impl_to_wkt!(as_large_multi_polygon, 3),
            Mixed(_, XYZ) => impl_to_wkt!(as_mixed, 3),
            LargeMixed(_, XYZ) => impl_to_wkt!(as_large_mixed, 3),
            GeometryCollection(_, XYZ) => impl_to_wkt!(as_geometry_collection, 3),
            LargeGeometryCollection(_, XYZ) => impl_to_wkt!(as_large_geometry_collection, 3),
            Rect(XYZ) => impl_to_wkt!(as_rect, 3),
        }
    }
}
