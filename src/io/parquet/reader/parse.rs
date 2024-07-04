//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use crate::array::{
    from_arrow_array, CoordType, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{
    infer_geo_data_type, GeoParquetColumnMetadata, GeoParquetMetadata,
};
use crate::io::wkb::from_wkb;
use crate::GeometryArrayTrait;

pub fn infer_target_schema(
    existing_schema: &Schema,
    geo_meta: &GeoParquetMetadata,
) -> Result<SchemaRef> {
    let mut new_fields: Vec<FieldRef> = Vec::with_capacity(existing_schema.fields().len());
    for existing_field in existing_schema.fields() {
        if let Some(column_meta) = geo_meta.columns.get(existing_field.name()) {
            new_fields.push(infer_target_field(existing_field, column_meta)?)
        } else {
            new_fields.push(existing_field.clone());
        }
    }

    Ok(Arc::new(Schema::new_with_metadata(
        new_fields,
        existing_schema.metadata().clone(),
    )))
}

fn infer_target_field(
    existing_field: &Field,
    column_meta: &GeoParquetColumnMetadata,
) -> Result<FieldRef> {
    let target_geo_data_type: GeoDataType = match column_meta.encoding.as_str() {
        "WKB" => infer_target_wkb_type(&column_meta.geometry_types)?,
        "point" => GeoDataType::Point(CoordType::Separated),
        "linestring" => GeoDataType::LineString(CoordType::Separated),
        "polygon" => GeoDataType::Polygon(CoordType::Separated),
        "multipoint" => GeoDataType::MultiPoint(CoordType::Separated),
        "multilinestring" => GeoDataType::MultiLineString(CoordType::Separated),
        "multipolygon" => GeoDataType::MultiPolygon(CoordType::Separated),
        other => {
            return Err(GeoArrowError::General(format!(
                "Unexpected GeoParquet encoding {}",
                other
            )))
        }
    };
    Ok(Arc::new(target_geo_data_type.to_field_with_metadata(
        existing_field.name(),
        existing_field.is_nullable(),
        &column_meta.into(),
    )))
}

fn infer_target_wkb_type(geometry_types: &[String]) -> Result<GeoDataType> {
    let mut geom_types_set = HashSet::new();
    for t in geometry_types {
        geom_types_set.insert(t.as_str());
    }
    Ok(
        infer_geo_data_type(&geom_types_set, CoordType::Interleaved)?
            .unwrap_or(GeoDataType::Mixed(CoordType::Interleaved)),
    )
}

/// Parse a record batch to a GeoArrow record batch.
pub fn parse_record_batch(batch: RecordBatch, target_schema: SchemaRef) -> Result<RecordBatch> {
    let orig_columns = batch.columns().to_vec();
    let mut output_columns = Vec::with_capacity(orig_columns.len());

    for ((orig_field, target_field), column) in batch
        .schema_ref()
        .fields()
        .iter()
        .zip(target_schema.fields())
        .zip(orig_columns)
    {
        // Invariant: the target schema has the same column ordering as the original, just that
        // some fields are desired to be parsed.
        assert_eq!(orig_field.name(), target_field.name());

        if orig_field.data_type() != target_field.data_type()
            || orig_field.metadata() != target_field.metadata()
        {
            let output_column = parse_array(column.as_ref(), orig_field, target_field)?;
            output_columns.push(output_column);
        } else {
            output_columns.push(column);
        }
    }

    Ok(RecordBatch::try_new(target_schema, output_columns)?)
}

/// Parse a single column based on provided GeoParquet metadata and target field
fn parse_array(
    array: &dyn Array,
    orig_field: &Field,
    target_field: &Field,
) -> Result<Arc<dyn Array>> {
    use GeoDataType::*;
    let geo_arr = from_arrow_array(array, orig_field)?;
    match geo_arr.data_type() {
        WKB | LargeWKB => parse_wkb_column(array, target_field),
        Point(_) => parse_point_column(array),
        LineString(_) | LargeLineString(_) => parse_line_string_column(array),
        Polygon(_) | LargePolygon(_) => parse_polygon_column(array),
        MultiPoint(_) | LargeMultiPoint(_) => parse_multi_point_column(array),
        MultiLineString(_) | LargeMultiLineString(_) => parse_multi_line_string_column(array),
        MultiPolygon(_) | LargeMultiPolygon(_) => parse_multi_polygon_column(array),
        other => Err(GeoArrowError::General(format!(
            "Unexpected geometry encoding: {:?}",
            other
        ))),
    }
}

fn parse_wkb_column(arr: &dyn Array, target_field: &Field) -> Result<Arc<dyn Array>> {
    let target_geo_data_type: GeoDataType = target_field.try_into()?;
    match arr.data_type() {
        DataType::Binary => {
            let wkb_arr = WKBArray::<i32>::try_from(arr)?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        DataType::LargeBinary => {
            let wkb_arr = WKBArray::<i64>::try_from(arr)?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        dt => Err(GeoArrowError::General(format!(
            "Expected WKB array to have binary data type, got {}",
            dt
        ))),
    }
}

fn parse_point_column(arr: &dyn Array) -> Result<Arc<dyn Array>> {
    let geom_arr: PointArray = arr.try_into()?;
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $small_geoarrow_type:ty, $large_geoarrow_type:ty) => {
        fn $fn_name(arr: &dyn Array) -> Result<Arc<dyn Array>> {
            match arr.data_type() {
                DataType::List(_) => {
                    let geom_arr: $small_geoarrow_type = arr.try_into()?;
                    Ok(geom_arr.into_array_ref())
                }
                DataType::LargeList(_) => {
                    let geom_arr: $large_geoarrow_type = arr.try_into()?;
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
