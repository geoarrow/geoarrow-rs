use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::array::CoordType;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};

use arrow_schema::Schema;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::file::metadata::FileMetaData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct GeoParquetMetadata {
    pub version: String,
    pub primary_column: String,
    pub columns: HashMap<String, GeoParquetColumnMetadata>,
}

#[derive(Serialize, Deserialize)]
pub struct GeoParquetColumnMetadata {
    pub encoding: String,
    pub geometry_types: Vec<String>,
    pub crs: Option<Value>,
    pub orientation: Option<String>,
    pub edges: Option<String>,
    pub bbox: Option<Vec<f64>>,
    pub epoch: Option<i32>,
}

impl GeoParquetMetadata {
    pub fn from_parquet_meta(metadata: &FileMetaData) -> Result<Self> {
        let kv_metadata = metadata.key_value_metadata();

        if let Some(metadata) = kv_metadata {
            for kv in metadata {
                if kv.key == "geo" {
                    if let Some(value) = &kv.value {
                        return Ok(serde_json::from_str(value)?);
                    }
                }
            }
        }

        Err(GeoArrowError::General(
            "expected a 'geo' key in GeoParquet metadata".to_string(),
        ))
    }
}

// TODO: deduplicate with `resolve_types` in `downcast.rs`
fn infer_geo_data_type(
    geometry_types: &HashSet<&str>,
    coord_type: CoordType,
) -> Result<Option<GeoDataType>> {
    if geometry_types.iter().any(|t| t.contains(" Z")) {
        return Err(GeoArrowError::General(
            "3D coordinates not currently supported".to_string(),
        ));
    }

    match geometry_types.len() {
        0 => Ok(None),
        1 => Ok(Some(match *geometry_types.iter().next().unwrap() {
            "Point" => GeoDataType::Point(coord_type),
            "LineString" => GeoDataType::LineString(coord_type),
            "Polygon" => GeoDataType::Polygon(coord_type),
            "MultiPoint" => GeoDataType::MultiPoint(coord_type),
            "MultiLineString" => GeoDataType::MultiLineString(coord_type),
            "MultiPolygon" => GeoDataType::MultiPolygon(coord_type),
            "GeometryCollection" => GeoDataType::GeometryCollection(coord_type),
            _ => unreachable!(),
        })),
        2 => {
            if geometry_types.contains("Point") && geometry_types.contains("MultiPoint") {
                Ok(Some(GeoDataType::MultiPoint(coord_type)))
            } else if geometry_types.contains("LineString")
                && geometry_types.contains("MultiLineString")
            {
                Ok(Some(GeoDataType::MultiLineString(coord_type)))
            } else if geometry_types.contains("Polygon") && geometry_types.contains("MultiPolygon")
            {
                Ok(Some(GeoDataType::MultiPolygon(coord_type)))
            } else {
                Ok(Some(GeoDataType::Mixed(coord_type)))
            }
        }
        _ => Ok(Some(GeoDataType::Mixed(coord_type))),
    }
}

fn parse_geoparquet_metadata(
    metadata: &FileMetaData,
    schema: &Schema,
    coord_type: CoordType,
) -> Result<(usize, Option<GeoDataType>)> {
    let meta = GeoParquetMetadata::from_parquet_meta(metadata)?;
    let column_meta = meta
        .columns
        .get(&meta.primary_column)
        .ok_or(GeoArrowError::General(format!(
            "Expected {} in GeoParquet column metadata",
            &meta.primary_column
        )))?;

    let geometry_column_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == &meta.primary_column)
        .unwrap();
    let mut geometry_types = HashSet::with_capacity(column_meta.geometry_types.len());
    column_meta.geometry_types.iter().for_each(|t| {
        geometry_types.insert(t.as_str());
    });
    Ok((
        geometry_column_index,
        infer_geo_data_type(&geometry_types, coord_type)?,
    ))
}

pub fn build_arrow_schema<T>(
    builder: &ArrowReaderBuilder<T>,
    coord_type: &CoordType,
) -> Result<(Arc<Schema>, usize, Option<GeoDataType>)> {
    let parquet_meta = builder.metadata();
    let arrow_schema = builder.schema().clone();
    let (geometry_column_index, target_geo_data_type) =
        parse_geoparquet_metadata(parquet_meta.file_metadata(), &arrow_schema, *coord_type)?;
    Ok((arrow_schema, geometry_column_index, target_geo_data_type))
}
