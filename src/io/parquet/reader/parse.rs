//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::array::metadata::ArrayMetadata;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WKBArray,
};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{GeoParquetColumnMetadata, GeoParquetMetadata};
use crate::io::wkb::from_wkb;
use crate::GeometryArrayTrait;

pub fn infer_target_schema(existing_schema: &Schema, geo_meta: &GeoParquetMetadata) -> SchemaRef {
    todo!()
}

/// Parse a record batch to a GeoArrow record batch.
pub fn parse_record_batch(
    batch: RecordBatch,
    // column_metas: HashMap<usize, GeoParquetColumnMetadata>,
    target_schema: SchemaRef,
) -> Result<RecordBatch> {
    // if

    let mut new_columns = batch.columns().to_vec();
    for (column_idx, column_meta) in column_metas.iter() {
        let target_field = target_schema.field(*column_idx);
        let array = batch.column(*column_idx);
        let parsed_column = parse_column(array, column_meta, target_field)?;
        new_columns[*column_idx] = parsed_column;
    }
    Ok(RecordBatch::try_new(target_schema, new_columns)?)
}

/// Parse a single column based on provided GeoParquet metadata and target field
fn parse_column(
    arr: &dyn Array,
    // column_meta: &GeoParquetColumnMetadata,
    target_field: &Field,
) -> Result<Arc<dyn Array>> {
    // TODO: infer array metadata from target field extension metadata
    let array_metadata: Arc<ArrayMetadata> = Arc::new(column_meta.into());

    match column_meta.encoding.as_str() {
        "WKB" => parse_wkb_column(arr, target_field, array_metadata),
        "point" => parse_point_column(arr, array_metadata),
        "linestring" => parse_line_string_column(arr, array_metadata),
        "polygon" => parse_polygon_column(arr, array_metadata),
        "multipoint" => parse_multi_point_column(arr, array_metadata),
        "multilinestring" => parse_multi_line_string_column(arr, array_metadata),
        "multipolygon" => parse_multi_polygon_column(arr, array_metadata),
        other => Err(GeoArrowError::General(format!(
            "Unexpected geometry encoding: {}",
            other
        ))),
    }
}

fn parse_wkb_column(
    arr: &dyn Array,
    target_field: &Field,
    array_metadata: Arc<ArrayMetadata>,
) -> Result<Arc<dyn Array>> {
    let target_geo_data_type: GeoDataType = target_field.try_into()?;
    match arr.data_type() {
        DataType::Binary => {
            let mut wkb_arr = WKBArray::<i32>::try_from(arr)?;
            wkb_arr.metadata = array_metadata;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        DataType::LargeBinary => {
            let mut wkb_arr = WKBArray::<i64>::try_from(arr)?;
            wkb_arr.metadata = array_metadata;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        dt => Err(GeoArrowError::General(format!(
            "Expected WKB array to have binary data type, got {}",
            dt
        ))),
    }
}

fn parse_point_column(
    arr: &dyn Array,
    array_metadata: Arc<ArrayMetadata>,
) -> Result<Arc<dyn Array>> {
    let mut geom_arr: PointArray = arr.try_into()?;
    geom_arr.metadata = array_metadata;
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $small_geoarrow_type:ty, $large_geoarrow_type:ty) => {
        fn $fn_name(arr: &dyn Array, array_metadata: Arc<ArrayMetadata>) -> Result<Arc<dyn Array>> {
            match arr.data_type() {
                DataType::List(_) => {
                    let mut geom_arr: $small_geoarrow_type = arr.try_into()?;
                    geom_arr.metadata = array_metadata;
                    Ok(geom_arr.into_array_ref())
                }
                DataType::LargeList(_) => {
                    let mut geom_arr: $large_geoarrow_type = arr.try_into()?;
                    geom_arr.metadata = array_metadata;
                    Ok(geom_arr.into_array_ref())
                }
                dt => Err(GeoArrowError::General(format!(
                    "Unexpected Arrow data type: {}",
                    dt
                ))),
            }
        }
    };
}

impl_parse_fn!(
    parse_line_string_column,
    LineStringArray<i32>,
    LineStringArray<i64>
);
impl_parse_fn!(parse_polygon_column, PolygonArray<i32>, PolygonArray<i64>);
impl_parse_fn!(
    parse_multi_point_column,
    MultiPointArray<i32>,
    MultiPointArray<i64>
);
impl_parse_fn!(
    parse_multi_line_string_column,
    MultiLineStringArray<i32>,
    MultiLineStringArray<i64>
);
impl_parse_fn!(
    parse_multi_polygon_column,
    MultiPolygonArray<i32>,
    MultiPolygonArray<i64>
);
