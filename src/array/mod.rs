//! Contains implementations of GeoArrow arrays.

pub use binary::{WKBArray, WKBBuilder};
pub use coord::{
    CoordBuffer, CoordBufferBuilder, CoordType, InterleavedCoordBuffer,
    InterleavedCoordBufferBuilder, SeparatedCoordBuffer, SeparatedCoordBufferBuilder,
};
pub use geometry::GeometryArray;
pub use geometrycollection::GeometryCollectionArray;
pub use linestring::{LineStringArray, LineStringBuilder};
pub use mixed::{MixedGeometryArray, MixedGeometryBuilder};
pub use multilinestring::{MultiLineStringArray, MultiLineStringBuilder};
pub use multipoint::{MultiPointArray, MultiPointBuilder};
pub use multipolygon::{MultiPolygonArray, MultiPolygonBuilder};
pub use point::{PointArray, PointBuilder};
pub use polygon::{PolygonArray, PolygonBuilder};
pub use rect::RectArray;

pub mod binary;
pub mod coord;
pub mod geometry;
pub mod geometrycollection;
pub mod linestring;
pub mod mixed;
pub mod multilinestring;
pub mod multipoint;
pub mod multipolygon;
pub mod offset_builder;
pub mod point;
pub mod polygon;
pub mod rect;
pub mod util;
pub mod zip_validity;

use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::{DataType, Field};

use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;

/// Convert an Arrow [Array] to a geoarrow GeometryArray
pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Arc<dyn GeometryArrayTrait>> {
    if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
        let geom_arr: Arc<dyn GeometryArrayTrait> = match extension_name.as_str() {
            "geoarrow.point" => Arc::new(PointArray::try_from(array).unwrap()),
            "geoarrow.linestring" => match field.data_type() {
                DataType::List(_) => Arc::new(LineStringArray::<i32>::try_from(array).unwrap()),
                DataType::LargeList(_) => {
                    Arc::new(LineStringArray::<i64>::try_from(array).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.polygon" => match field.data_type() {
                DataType::List(_) => Arc::new(PolygonArray::<i32>::try_from(array).unwrap()),
                DataType::LargeList(_) => Arc::new(PolygonArray::<i64>::try_from(array).unwrap()),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multipoint" => match field.data_type() {
                DataType::List(_) => Arc::new(MultiPointArray::<i32>::try_from(array).unwrap()),
                DataType::LargeList(_) => {
                    Arc::new(MultiPointArray::<i64>::try_from(array).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multilinestring" => match field.data_type() {
                DataType::List(_) => {
                    Arc::new(MultiLineStringArray::<i32>::try_from(array).unwrap())
                }
                DataType::LargeList(_) => {
                    Arc::new(MultiLineStringArray::<i64>::try_from(array).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multipolygon" => match field.data_type() {
                DataType::List(_) => Arc::new(MultiPolygonArray::<i32>::try_from(array).unwrap()),
                DataType::LargeList(_) => {
                    Arc::new(MultiPolygonArray::<i64>::try_from(array).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.geometry" => match field.data_type() {
                DataType::Union(fields, _) => {
                    let mut large_offsets: Vec<bool> = vec![];

                    fields.iter().for_each(|(_type_ids, field)| {
                        match field.data_type() {
                            DataType::List(_) => large_offsets.push(false),
                            DataType::LargeList(_) => large_offsets.push(true),
                            _ => (),
                        };
                    });

                    if large_offsets.is_empty() {
                        // Only contains a point array, we can cast to i32
                        Arc::new(MixedGeometryArray::<i32>::try_from(array).unwrap())
                    } else if large_offsets.iter().all(|x| *x) {
                        // All large offsets, cast to i64
                        Arc::new(MixedGeometryArray::<i64>::try_from(array).unwrap())
                    } else if large_offsets.iter().all(|x| !x) {
                        // All small offsets, cast to i32
                        Arc::new(MixedGeometryArray::<i32>::try_from(array).unwrap())
                    } else {
                        panic!("Mix of offset types");
                    }
                }
                DataType::LargeList(_) => {
                    Arc::new(MultiPolygonArray::<i64>::try_from(array).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.geometrycollection" => match field.data_type() {
                DataType::List(_) => {
                    Arc::new(GeometryCollectionArray::<i32>::try_from(array).unwrap())
                }
                DataType::LargeList(_) => {
                    Arc::new(GeometryCollectionArray::<i64>::try_from(array).unwrap())
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.wkb" => match field.data_type() {
                DataType::Binary => Arc::new(WKBArray::<i32>::try_from(array).unwrap()),
                DataType::LargeBinary => Arc::new(WKBArray::<i64>::try_from(array).unwrap()),
                _ => panic!("Unexpected data type"),
            },
            _ => {
                return Err(GeoArrowError::General(format!(
                    "Unknown geoarrow type {}",
                    extension_name
                )))
            }
        };
        Ok(geom_arr)
    } else {
        // TODO: better error here, and document that arrays without geoarrow extension
        // metadata should use TryFrom for a specific geometry type directly, instead of using
        // GeometryArray
        match field.data_type() {
            DataType::Binary => {
                Ok(Arc::new(WKBArray::<i32>::try_from(array).unwrap()))
            }
            DataType::LargeBinary => {
                Ok(Arc::new(WKBArray::<i64>::try_from(array).unwrap()))
            }
            DataType::Struct(_) => {
                Ok(Arc::new(PointArray::try_from(array).unwrap()))
            }
            DataType::FixedSizeList(_, _) => {
                Ok(Arc::new(PointArray::try_from(array).unwrap()))
            }
            _ => Err(GeoArrowError::General("Only Binary, LargeBinary, FixedSizeList, and Struct arrays are unambigously typed and can be used without extension metadata.".to_string()))
        }
    }
}
