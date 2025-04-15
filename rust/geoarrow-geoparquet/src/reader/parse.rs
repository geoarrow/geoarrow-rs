//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, OffsetSizeTrait, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WkbArray,
};
use geoarrow_array::builder::{
    GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
    MultiPointBuilder, MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};
use geoarrow_schema::{
    CoordType, Dimension, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
};

use crate::metadata::{
    GeoParquetColumnEncoding, GeoParquetColumnMetadata, GeoParquetGeometryType, GeoParquetMetadata,
    infer_geo_data_type,
};
use geoarrow_array::error::{GeoArrowError, Result};

pub fn infer_target_schema(
    existing_schema: &Schema,
    geo_meta: &GeoParquetMetadata,
    coord_type: CoordType,
) -> Result<SchemaRef> {
    let mut new_fields: Vec<FieldRef> = Vec::with_capacity(existing_schema.fields().len());
    for existing_field in existing_schema.fields() {
        if let Some(column_meta) = geo_meta.columns.get(existing_field.name()) {
            new_fields.push(infer_target_field(existing_field, column_meta, coord_type)?)
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
    coord_type: CoordType,
) -> Result<FieldRef> {
    let metadata = Arc::new(Metadata::from(column_meta.clone()));

    let target_geo_data_type: GeoArrowType = match column_meta.encoding {
        GeoParquetColumnEncoding::WKB => {
            infer_target_wkb_type(&column_meta.geometry_types, coord_type)?
        }
        GeoParquetColumnEncoding::Point => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PointZ)
            {
                GeoArrowType::Point(PointType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::Point(PointType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::LineString => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
            {
                GeoArrowType::LineString(LineStringType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::LineString(LineStringType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::Polygon => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
            {
                GeoArrowType::Polygon(PolygonType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::Polygon(PolygonType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::MultiPoint => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PointZ)
                || column_meta
                    .geometry_types
                    .contains(&GeoParquetGeometryType::MultiPointZ)
            {
                GeoArrowType::MultiPoint(MultiPointType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::MultiPoint(MultiPointType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::MultiLineString => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
                || column_meta
                    .geometry_types
                    .contains(&GeoParquetGeometryType::MultiLineStringZ)
            {
                GeoArrowType::MultiLineString(MultiLineStringType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::MultiLineString(MultiLineStringType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::MultiPolygon => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PolygonZ)
                || column_meta
                    .geometry_types
                    .contains(&GeoParquetGeometryType::MultiPolygonZ)
            {
                GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
    };

    Ok(Arc::new(target_geo_data_type.to_field(
        existing_field.name(),
        existing_field.is_nullable(),
    )))
}

fn infer_target_wkb_type(
    geometry_types: &HashSet<GeoParquetGeometryType>,
    coord_type: CoordType,
) -> Result<GeoArrowType> {
    Ok(
        infer_geo_data_type(geometry_types, coord_type)?.unwrap_or(GeoArrowType::Geometry(
            GeometryType::new(coord_type, Default::default()),
        )),
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
            let output_column = parse_array(column, orig_field, target_field)?;
            output_columns.push(output_column);
        } else {
            output_columns.push(column);
        }
    }

    Ok(RecordBatch::try_new(target_schema, output_columns)?)
}

/// Parse a single column based on provided GeoParquet metadata and target field
fn parse_array(array: ArrayRef, orig_field: &Field, target_field: &Field) -> Result<ArrayRef> {
    let orig_type = GeoArrowType::try_from(orig_field)?;
    let arr = array.as_ref();
    match orig_type {
        GeoArrowType::Point(typ) => parse_point_column(arr, typ),
        GeoArrowType::LineString(typ) => parse_line_string_column(arr, typ),
        GeoArrowType::Polygon(typ) => parse_polygon_column(arr, typ),
        GeoArrowType::MultiPoint(typ) => parse_multi_point_column(arr, typ),
        GeoArrowType::MultiLineString(typ) => parse_multi_line_string_column(arr, typ),
        GeoArrowType::MultiPolygon(typ) => parse_multi_polygon_column(arr, typ),
        GeoArrowType::Wkb(_) | GeoArrowType::LargeWkb(_) => {
            parse_wkb_column(arr, target_field.try_into()?)
        }
        GeoArrowType::Wkt(_) | GeoArrowType::LargeWkt(_) => Err(GeoArrowError::General(
            "WKT input not supported in GeoParquet.".to_string(),
        )),
        other => Err(GeoArrowError::General(format!(
            "Unexpected geometry encoding: {:?}",
            other
        ))),
    }
}

fn parse_wkb_column(arr: &dyn Array, target_geo_data_type: GeoArrowType) -> Result<ArrayRef> {
    match arr.data_type() {
        DataType::Binary => {
            let wkb_arr = WkbArray::<i32>::try_from((arr, WkbType::new(Default::default())))?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        DataType::LargeBinary => {
            let wkb_arr = WkbArray::<i64>::try_from((arr, WkbType::new(Default::default())))?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        dt => Err(GeoArrowError::General(format!(
            "Expected WKB array to have binary data type, got {}",
            dt
        ))),
    }
}

fn parse_point_column(array: &dyn Array, typ: PointType) -> Result<ArrayRef> {
    let geom_arr: PointArray = (array, typ).try_into()?;
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $geoarrow_type:ty, $geom_type:ty) => {
        fn $fn_name(array: &dyn Array, typ: $geom_type) -> Result<ArrayRef> {
            match array.data_type() {
                DataType::List(_) | DataType::LargeList(_) => {
                    let geom_arr: $geoarrow_type = (array, typ).try_into()?;
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

/// Parse a [WKBArray] to a GeometryArray with GeoArrow native encoding.
///
/// This supports either ISO or EWKB-flavored data.
///
/// The returned array is guaranteed to have exactly the type of `target_type`.
///
/// `GeoArrowType::Rect` is currently not allowed.
fn from_wkb<O: OffsetSizeTrait>(
    arr: &WkbArray<O>,
    target_type: GeoArrowType,
    prefer_multi: bool,
) -> Result<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;

    let geoms = arr
        .iter()
        .map(|x| x.transpose())
        .collect::<Result<Vec<Option<_>>>>()?;

    match target_type {
        Point(typ) => {
            let builder = PointBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(typ) => {
            let builder = LineStringBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(typ) => {
            let builder = PolygonBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(typ) => {
            let builder = MultiPointBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(typ) => {
            let builder = MultiLineStringBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(typ) => {
            let builder = MultiPolygonBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(typ) => {
            let builder =
                GeometryCollectionBuilder::from_nullable_geometries(&geoms, typ, prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        Rect(_) => Err(GeoArrowError::General(format!(
            "Unexpected data type {:?}",
            target_type,
        ))),
        Geometry(typ) => {
            let builder = GeometryBuilder::from_nullable_geometries(&geoms, typ, prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        _ => todo!("Handle target WKB/WKT in `from_wkb`"),
    }
}
