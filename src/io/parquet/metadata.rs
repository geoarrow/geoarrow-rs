use std::collections::{HashMap, HashSet};

use crate::array::metadata::{ArrayMetadata, Edges};
use crate::array::CoordType;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};

use arrow_schema::Schema;
use parquet::file::metadata::FileMetaData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoParquetMetadata {
    pub version: String,
    pub primary_column: String,
    pub columns: HashMap<String, GeoParquetColumnMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoParquetColumnMetadata {
    pub encoding: String,
    pub geometry_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crs: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orientation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edges: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

    pub fn try_update(&mut self, other: &FileMetaData) -> Result<()> {
        let other = Self::from_parquet_meta(other)?;
        self.try_compatible_with(&other)?;
        for (column_name, column_meta) in self.columns.iter_mut() {
            let other_column_meta = other.columns.get(column_name.as_str()).unwrap();
            match (column_meta.bbox.as_mut(), &other_column_meta.bbox) {
                (Some(bbox), Some(other_bbox)) => {
                    assert_eq!(bbox.len(), other_bbox.len());
                    if bbox.len() == 4 {
                        if other_bbox[0] < bbox[0] {
                            bbox[0] = other_bbox[0];
                        }
                        if other_bbox[1] < bbox[1] {
                            bbox[1] = other_bbox[1];
                        }
                        if other_bbox[2] > bbox[2] {
                            bbox[2] = other_bbox[2];
                        }
                        if other_bbox[3] > bbox[3] {
                            bbox[3] = other_bbox[3];
                        }
                    } else if bbox.len() == 6 {
                        if other_bbox[0] < bbox[0] {
                            bbox[0] = other_bbox[0];
                        }
                        if other_bbox[1] < bbox[1] {
                            bbox[1] = other_bbox[1];
                        }
                        if other_bbox[2] < bbox[2] {
                            bbox[2] = other_bbox[2];
                        }
                        if other_bbox[3] > bbox[3] {
                            bbox[3] = other_bbox[3];
                        }
                        if other_bbox[4] > bbox[4] {
                            bbox[4] = other_bbox[4];
                        }
                        if other_bbox[5] > bbox[5] {
                            bbox[5] = other_bbox[5];
                        }
                    }
                }
                (None, Some(other_bbox)) => {
                    column_meta.bbox = Some(other_bbox.clone());
                }
                // If the RHS doesn't have a bbox, we don't need to update
                (_, None) => {}
            }
        }
        Ok(())
    }

    /// Check if this metadata is compatible with another metadata instance, swallowing the error
    /// message if not compatible.
    pub fn is_compatible_with(&self, other: &GeoParquetMetadata) -> bool {
        self.try_compatible_with(other).is_ok()
    }

    /// Assert that this metadata is compatible with another metadata instance, erroring if not
    pub fn try_compatible_with(&self, other: &GeoParquetMetadata) -> Result<()> {
        if self.version.as_str() != other.version.as_str() {
            return Err(GeoArrowError::General(
                "Different GeoParquet versions".to_string(),
            ));
        }

        if self.primary_column.as_str() != other.primary_column.as_str() {
            return Err(GeoArrowError::General(
                "Different GeoParquet primary columns".to_string(),
            ));
        }

        for key in self.columns.keys() {
            let left = self.columns.get(key).unwrap();
            let right = other
                .columns
                .get(key)
                .ok_or(GeoArrowError::General(format!(
                    "Other GeoParquet metadata missing column {}",
                    key
                )))?;

            if left.encoding.as_str() != right.encoding.as_str() {
                return Err(GeoArrowError::General(format!(
                    "Different GeoParquet encodings for column {}",
                    key
                )));
            }

            if left.geometry_types != right.geometry_types {
                return Err(GeoArrowError::General(format!(
                    "Different GeoParquet geometry types for column {}",
                    key
                )));
            }

            if let (Some(left_bbox), Some(right_bbox)) = (&left.bbox, &right.bbox) {
                if left_bbox.len() != right_bbox.len() {
                    return Err(GeoArrowError::General(format!(
                        "Different bbox dimensions for column {}",
                        key
                    )));
                }
            }

            match (left.crs.as_ref(), right.crs.as_ref()) {
                (Some(left_crs), Some(right_crs)) => {
                    if left_crs != right_crs {
                        return Err(GeoArrowError::General(format!(
                            "Different GeoParquet CRS for column {}",
                            key
                        )));
                    }
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(GeoArrowError::General(format!(
                        "Different GeoParquet CRS for column {}",
                        key
                    )));
                }
                (None, None) => (),
            }
        }

        Ok(())
    }
}

impl From<GeoParquetColumnMetadata> for ArrayMetadata {
    fn from(value: GeoParquetColumnMetadata) -> Self {
        let edges = if let Some(edges) = value.edges {
            if edges.as_str() == "spherical" {
                Some(Edges::Spherical)
            } else {
                None
            }
        } else {
            None
        };
        ArrayMetadata {
            crs: value.crs,
            edges,
        }
    }
}

impl From<&GeoParquetColumnMetadata> for ArrayMetadata {
    fn from(value: &GeoParquetColumnMetadata) -> Self {
        let edges = if let Some(edges) = &value.edges {
            if edges.as_str() == "spherical" {
                Some(Edges::Spherical)
            } else {
                None
            }
        } else {
            None
        };
        ArrayMetadata {
            crs: value.crs.clone(),
            edges,
        }
    }
}
// TODO: deduplicate with `resolve_types` in `downcast.rs`
pub(crate) fn infer_geo_data_type(
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
            "Point" => GeoDataType::Point(coord_type, Dimension::XY),
            "LineString" => GeoDataType::LineString(coord_type, Dimension::XY),
            "Polygon" => GeoDataType::Polygon(coord_type, Dimension::XY),
            "MultiPoint" => GeoDataType::MultiPoint(coord_type, Dimension::XY),
            "MultiLineString" => GeoDataType::MultiLineString(coord_type, Dimension::XY),
            "MultiPolygon" => GeoDataType::MultiPolygon(coord_type, Dimension::XY),
            "GeometryCollection" => GeoDataType::GeometryCollection(coord_type, Dimension::XY),
            _ => unreachable!(),
        })),
        2 => {
            if geometry_types.contains("Point") && geometry_types.contains("MultiPoint") {
                Ok(Some(GeoDataType::MultiPoint(coord_type, Dimension::XY)))
            } else if geometry_types.contains("LineString")
                && geometry_types.contains("MultiLineString")
            {
                Ok(Some(GeoDataType::MultiLineString(
                    coord_type,
                    Dimension::XY,
                )))
            } else if geometry_types.contains("Polygon") && geometry_types.contains("MultiPolygon")
            {
                Ok(Some(GeoDataType::MultiPolygon(coord_type, Dimension::XY)))
            } else {
                Ok(Some(GeoDataType::Mixed(coord_type, Dimension::XY)))
            }
        }
        _ => Ok(Some(GeoDataType::Mixed(coord_type, Dimension::XY))),
    }
}

/// Find all geometry columns in the Arrow schema, constructing their GeoDataTypes
pub(crate) fn find_geoparquet_geom_columns(
    metadata: &FileMetaData,
    schema: &Schema,
    coord_type: CoordType,
) -> Result<Vec<(usize, Option<GeoDataType>)>> {
    let meta = GeoParquetMetadata::from_parquet_meta(metadata)?;

    meta.columns
        .iter()
        .map(|(col_name, col_meta)| {
            let geometry_column_index = schema
                .fields()
                .iter()
                .position(|field| field.name().as_str() == col_name.as_str())
                .unwrap();
            let mut geometry_types = HashSet::with_capacity(col_meta.geometry_types.len());
            col_meta.geometry_types.iter().for_each(|t| {
                geometry_types.insert(t.as_str());
            });
            Ok((
                geometry_column_index,
                infer_geo_data_type(&geometry_types, coord_type)?,
            ))
        })
        .collect()
}
