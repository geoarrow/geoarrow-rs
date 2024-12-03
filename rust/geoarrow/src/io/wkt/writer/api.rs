use arrow::array::GenericStringBuilder;
use arrow_array::OffsetSizeTrait;

use crate::array::{AsChunkedNativeArray, AsNativeArray, WKTArray};
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
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

        use NativeType::*;

        macro_rules! impl_to_wkt {
            ($cast_func:ident, $to_wkt_func:expr) => {
                for maybe_geom in self.$cast_func().iter() {
                    output_array
                        .append_option(maybe_geom.map(|geom| $to_wkt_func(&geom).to_string()));
                }
            };
        }

        match self.data_type() {
            Point(_, _) => impl_to_wkt!(as_point, point_to_wkt),
            LineString(_, _) => impl_to_wkt!(as_line_string, line_string_to_wkt),
            Polygon(_, _) => impl_to_wkt!(as_polygon, polygon_to_wkt),
            MultiPoint(_, _) => impl_to_wkt!(as_multi_point, multi_point_to_wkt),
            MultiLineString(_, _) => {
                impl_to_wkt!(as_multi_line_string, multi_line_string_to_wkt)
            }
            MultiPolygon(_, _) => impl_to_wkt!(as_multi_polygon, multi_polygon_to_wkt),
            Mixed(_, _) => impl_to_wkt!(as_mixed, geometry_to_wkt),
            GeometryCollection(_, _) => {
                impl_to_wkt!(as_geometry_collection, geometry_collection_to_wkt)
            }
            Rect(_) => impl_to_wkt!(as_rect, rect_to_wkt),
            Unknown(_) => impl_to_wkt!(as_unknown, geometry_to_wkt),
        }

        WKTArray::new(output_array.finish(), metadata)
    }
}

impl ToWKT for &dyn ChunkedNativeArray {
    type Output<O: OffsetSizeTrait> = ChunkedGeometryArray<WKTArray<O>>;

    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use NativeType::*;

        macro_rules! impl_to_wkt {
            ($cast_func:ident) => {
                ChunkedGeometryArray::new(self.$cast_func().map(|chunk| chunk.as_ref().to_wkt()))
            };
        }

        match self.data_type() {
            Point(_, _) => impl_to_wkt!(as_point),
            LineString(_, _) => impl_to_wkt!(as_line_string),
            Polygon(_, _) => impl_to_wkt!(as_polygon),
            MultiPoint(_, _) => impl_to_wkt!(as_multi_point),
            MultiLineString(_, _) => impl_to_wkt!(as_multi_line_string),
            MultiPolygon(_, _) => impl_to_wkt!(as_multi_polygon),
            Mixed(_, _) => impl_to_wkt!(as_mixed),
            GeometryCollection(_, _) => impl_to_wkt!(as_geometry_collection),
            Rect(_) => impl_to_wkt!(as_rect),
            Unknown(_) => impl_to_wkt!(as_unknown),
        }
    }
}
