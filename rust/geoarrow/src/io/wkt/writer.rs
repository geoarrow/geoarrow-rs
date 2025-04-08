use arrow::array::GenericStringBuilder;
use arrow_array::OffsetSizeTrait;

use crate::NativeArray;
use crate::array::{AsChunkedNativeArray, AsNativeArray, WKTArray};
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use wkt::to_wkt::{
    write_geometry, write_geometry_collection, write_linestring, write_multi_linestring,
    write_multi_point, write_multi_polygon, write_point, write_polygon, write_rect,
};

/// Serialize a geometry array to Well-Known Text
pub trait ToWKT {
    /// The output type of the operation. You can specify whether you want to use i32 or i64
    /// offsets for the Arrow string array.
    type Output<O: OffsetSizeTrait>;

    /// Convert to WKT.
    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O>;
}

impl ToWKT for &dyn NativeArray {
    type Output<O: OffsetSizeTrait> = Result<WKTArray<O>>;

    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        let metadata = self.metadata();
        let mut output_array = GenericStringBuilder::<O>::new();

        use NativeType::*;

        macro_rules! impl_to_wkt {
            ($cast_func:ident, $write_wkt_func:expr) => {
                for maybe_geom in self.$cast_func().iter() {
                    if let Some(geom) = maybe_geom {
                        $write_wkt_func(&mut output_array, &geom)?;
                        output_array.append_value("");
                    } else {
                        output_array.append_null();
                    }
                }
            };
        }

        match self.data_type() {
            Point(_) => impl_to_wkt!(as_point, write_point),
            LineString(_) => impl_to_wkt!(as_line_string, write_linestring),
            Polygon(_) => impl_to_wkt!(as_polygon, write_polygon),
            MultiPoint(_) => impl_to_wkt!(as_multi_point, write_multi_point),
            MultiLineString(_) => {
                impl_to_wkt!(as_multi_line_string, write_multi_linestring)
            }
            MultiPolygon(_) => impl_to_wkt!(as_multi_polygon, write_multi_polygon),
            GeometryCollection(_) => {
                impl_to_wkt!(as_geometry_collection, write_geometry_collection)
            }
            Rect(_) => impl_to_wkt!(as_rect, write_rect),
            Geometry(_) => impl_to_wkt!(as_geometry, write_geometry),
        }

        Ok(WKTArray::new(output_array.finish(), metadata))
    }
}

impl ToWKT for &dyn ChunkedNativeArray {
    type Output<O: OffsetSizeTrait> = Result<ChunkedGeometryArray<WKTArray<O>>>;

    fn to_wkt<O: OffsetSizeTrait>(&self) -> Self::Output<O> {
        use NativeType::*;

        macro_rules! impl_to_wkt {
            ($cast_func:ident) => {{
                let chunks = self.$cast_func().try_map(|chunk| chunk.as_ref().to_wkt())?;
                Ok(ChunkedGeometryArray::new(chunks))
            }};
        }

        match self.data_type() {
            Point(_) => impl_to_wkt!(as_point),
            LineString(_) => impl_to_wkt!(as_line_string),
            Polygon(_) => impl_to_wkt!(as_polygon),
            MultiPoint(_) => impl_to_wkt!(as_multi_point),
            MultiLineString(_) => impl_to_wkt!(as_multi_line_string),
            MultiPolygon(_) => impl_to_wkt!(as_multi_polygon),
            GeometryCollection(_) => impl_to_wkt!(as_geometry_collection),
            Rect(_) => impl_to_wkt!(as_rect),
            Geometry(_) => impl_to_wkt!(as_geometry),
        }
    }
}
