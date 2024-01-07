use std::collections::HashMap;

use parquet::file::metadata::FileMetaData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{GeoArrowError, Result};

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
