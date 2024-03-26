//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema};

use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::GeoParquetColumnMetadata;
use crate::GeometryArrayTrait;

/// Parse a record batch to a GeoArrow record batch.
pub fn parse_record_batch(
    batch: &RecordBatch,
    column_metas: HashMap<usize, GeoParquetColumnMetadata>,
    new_schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let mut new_columns = batch.columns().to_vec();
    for (column_idx, column_meta) in column_metas.iter() {
        let array = batch.column(*column_idx);
        let parsed_column = parse_column(array, column_meta)?;
        new_columns[*column_idx] = parsed_column;
    }
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// Parse a single column based on provided GeoParquet metadata
fn parse_column(arr: &dyn Array, column_meta: &GeoParquetColumnMetadata) -> Result<Arc<dyn Array>> {
    match column_meta.encoding.as_str() {
        "WKB" => parse_wkb_column(arr, column_meta),
        "point" => parse_point_column(arr, column_meta),
        "linestring" => parse_line_string_column(arr, column_meta),
        "polygon" => parse_polygon_column(arr, column_meta),
        "multipoint" => parse_multi_point_column(arr, column_meta),
        "multilinestring" => parse_multi_line_string_column(arr, column_meta),
        "multipolygon" => parse_multi_polygon_column(arr, column_meta),
        other => Err(GeoArrowError::General(format!(
            "Unexpected geometry encoding: {}",
            other
        ))),
    }
}

fn parse_wkb_column(
    arr: &dyn Array,
    column_meta: &GeoParquetColumnMetadata,
) -> Result<Arc<dyn Array>> {
    todo!()
}

fn parse_point_column(
    arr: &dyn Array,
    column_meta: &GeoParquetColumnMetadata,
) -> Result<Arc<dyn Array>> {
    let mut geom_arr: PointArray = arr.try_into()?;
    geom_arr.metadata = Arc::new(column_meta.clone().into());
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $small_geoarrow_type:ty, $large_geoarrow_type:ty) => {
        fn $fn_name(
            arr: &dyn Array,
            column_meta: &GeoParquetColumnMetadata,
        ) -> Result<Arc<dyn Array>> {
            match arr.data_type() {
                DataType::List(_) => {
                    let mut geom_arr: $small_geoarrow_type = arr.try_into()?;
                    geom_arr.metadata = Arc::new(column_meta.clone().into());
                    Ok(geom_arr.into_array_ref())
                }
                DataType::LargeList(_) => {
                    let mut geom_arr: $large_geoarrow_type = arr.try_into()?;
                    geom_arr.metadata = Arc::new(column_meta.clone().into());
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
