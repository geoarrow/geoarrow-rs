//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{
    LargeWkbArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WkbArray, WkbViewArray,
};
use geoarrow_array::cast::from_wkb;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{
    CoordType, GeoArrowType, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
};

use crate::metadata::{
    GeoParquetColumnEncoding, GeoParquetColumnMetadata, GeoParquetGeometryTypeAndDimension,
    GeoParquetMetadata, infer_geo_data_type,
};

/// Given an Arrow schema and GeoParquet metadata, convert the schema to one with native GeoArrow
/// geometry types for each geometry column.
///
/// If `parse_to_native` is `false`, WKB geometries will be left alone (not parsed to a GeoArrow
/// native representation) but tagged with the `geoarrow.wkb` extension metadata.
pub fn infer_geoarrow_schema(
    existing_schema: &Schema,
    geo_meta: &GeoParquetMetadata,
    parse_to_native: bool,
    coord_type: CoordType,
) -> GeoArrowResult<SchemaRef> {
    let mut new_fields: Vec<FieldRef> = Vec::with_capacity(existing_schema.fields().len());
    for existing_field in existing_schema.fields() {
        if let Some(column_meta) = geo_meta.columns.get(existing_field.name()) {
            new_fields.push(infer_target_field(
                existing_field,
                column_meta,
                parse_to_native,
                coord_type,
            )?)
        } else {
            new_fields.push(existing_field.clone());
        }
    }

    Ok(Arc::new(Schema::new_with_metadata(
        new_fields,
        existing_schema.metadata().clone(),
    )))
}

/// For native encodings we always load to the separated encoding so that we don't need an extra
/// copy.
fn infer_target_field(
    existing_field: &Field,
    column_meta: &GeoParquetColumnMetadata,
    parse_to_native: bool,
    coord_type: CoordType,
) -> GeoArrowResult<FieldRef> {
    let metadata = Arc::new(Metadata::from(column_meta.clone()));

    let target_geo_data_type: GeoArrowType = match column_meta.encoding {
        GeoParquetColumnEncoding::WKB => {
            if parse_to_native {
                infer_target_wkb_type(&column_meta.geometry_types, coord_type, metadata)?
            } else {
                GeoArrowType::Wkb(WkbType::new(metadata))
            }
        }
        // For native encodings there should only be one geometry type
        _ => {
            assert_eq!(column_meta.geometry_types.len(), 1);
            let gpq_type = column_meta.geometry_types.iter().next().unwrap();
            gpq_type.to_data_type(coord_type, metadata)
        }
    };

    Ok(Arc::new(target_geo_data_type.to_field(
        existing_field.name(),
        existing_field.is_nullable(),
    )))
}

fn infer_target_wkb_type(
    geometry_types: &HashSet<GeoParquetGeometryTypeAndDimension>,
    coord_type: CoordType,
    metadata: Arc<Metadata>,
) -> GeoArrowResult<GeoArrowType> {
    Ok(
        infer_geo_data_type(geometry_types, coord_type, metadata.clone())?.unwrap_or(
            GeoArrowType::Geometry(GeometryType::new(metadata).with_coord_type(coord_type)),
        ),
    )
}

pub(crate) fn validate_target_schema(
    orig_schema: &Schema,
    target_schema: &Schema,
) -> GeoArrowResult<()> {
    if orig_schema.fields().len() != target_schema.fields().len() {
        return Err(GeoArrowError::GeoParquet(format!(
            "Expected reader schema and target schema to have same number of fields, but reader schema has {} and target schema has {}",
            orig_schema.fields().len(),
            target_schema.fields().len()
        )));
    }

    for (position, (orig_field, target_field)) in orig_schema
        .fields()
        .iter()
        .zip(target_schema.fields())
        .enumerate()
    {
        if orig_field.name() != target_field.name() {
            return Err(GeoArrowError::GeoParquet(format!(
                "Expected reader schema and target schema to have the same field name at each position, but position {} has reader name {} and target name {}.\nOnly the field data types and metadata may change to signify parsing to a GeoArrow native type.",
                position,
                orig_field.name(),
                target_field.name()
            )));
        }
    }

    Ok(())
}

/// Parse a record batch to a GeoArrow record batch.
pub(crate) fn parse_record_batch(
    batch: RecordBatch,
    target_schema: SchemaRef,
) -> GeoArrowResult<RecordBatch> {
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
            let output_column = parse_array(column, orig_field, target_field)?;
            output_columns.push(output_column);
        } else {
            output_columns.push(column);
        }
    }

    Ok(RecordBatch::try_new(target_schema, output_columns)?)
}

/// Parse a single column based on provided GeoParquet metadata and target field
fn parse_array(
    array: ArrayRef,
    orig_field: &Field,
    target_field: &Field,
) -> GeoArrowResult<ArrayRef> {
    let target_type = GeoArrowType::try_from(target_field)?;
    match orig_field.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            parse_wkb_column(array.as_ref(), target_field.try_into()?)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Err(
            GeoArrowError::GeoParquet("WKT input not supported in GeoParquet.".to_string()),
        ),
        // TODO: this is probably wrong; we should parse the fields based on the _input_ data type
        // (which will always have separated coordinate type) and then cast them to the output type
        // (which may have interleaved coordinate type.)
        _ => match target_type {
            GeoArrowType::Point(typ) => parse_point_column(&array, typ),
            GeoArrowType::LineString(typ) => parse_line_string_column(&array, typ),
            GeoArrowType::Polygon(typ) => parse_polygon_column(&array, typ),
            GeoArrowType::MultiPoint(typ) => parse_multi_point_column(&array, typ),
            GeoArrowType::MultiLineString(typ) => parse_multi_line_string_column(&array, typ),
            GeoArrowType::MultiPolygon(typ) => parse_multi_polygon_column(&array, typ),
            _ => unreachable!(),
        },
    }
}

fn parse_wkb_column(
    arr: &dyn Array,
    target_geo_data_type: GeoArrowType,
) -> GeoArrowResult<ArrayRef> {
    let metadata = target_geo_data_type.metadata().clone();
    match arr.data_type() {
        DataType::Binary => {
            let wkb_arr = WkbArray::try_from((arr, WkbType::new(metadata)))?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type)?;
            Ok(geom_arr.to_array_ref())
        }
        DataType::LargeBinary => {
            let wkb_arr = LargeWkbArray::try_from((arr, WkbType::new(metadata)))?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type)?;
            Ok(geom_arr.to_array_ref())
        }
        DataType::BinaryView => {
            let wkb_arr = WkbViewArray::try_from((arr, WkbType::new(metadata)))?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type)?;
            Ok(geom_arr.to_array_ref())
        }
        dt => Err(GeoArrowError::GeoParquet(format!(
            "Expected WKB input array to have binary data type, got {dt}",
        ))),
    }
}

fn parse_point_column(array: &dyn Array, typ: PointType) -> GeoArrowResult<ArrayRef> {
    let geom_arr: PointArray = (array, typ).try_into()?;
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $geoarrow_type:ty, $geom_type:ty) => {
        fn $fn_name(array: &dyn Array, typ: $geom_type) -> GeoArrowResult<ArrayRef> {
            let geom_arr: $geoarrow_type = (array, typ).try_into()?;
            Ok(geom_arr.into_array_ref())
        }
    };
}

impl_parse_fn!(parse_line_string_column, LineStringArray, LineStringType);
impl_parse_fn!(parse_polygon_column, PolygonArray, PolygonType);
impl_parse_fn!(parse_multi_point_column, MultiPointArray, MultiPointType);
impl_parse_fn!(
    parse_multi_line_string_column,
    MultiLineStringArray,
    MultiLineStringType
);
impl_parse_fn!(
    parse_multi_polygon_column,
    MultiPolygonArray,
    MultiPolygonType
);
