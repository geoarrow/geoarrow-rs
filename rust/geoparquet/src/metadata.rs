//! Structs corresponding to the metadata defined by the [GeoParquet specification].
//!
//! [GeoParquet specification]: https://geoparquet.org/releases/v1.1.0/

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use geo_traits::GeometryTrait;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{
    CoordType, Crs, Dimension, Edges, GeoArrowType, GeometryCollectionType, GeometryType,
    LineStringType, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType,
    PolygonType,
};
use parquet::basic::{EdgeInterpolationAlgorithm, LogicalType};
use parquet::file::metadata::{FileMetaData, KeyValue};
use parquet::schema::types::SchemaDescriptor;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::writer::GeoParquetWriterEncoding;

// https://github.com/geoarrow/geoarrow-rs/pull/1159#issuecomment-2904610370
pub(crate) const INFERRED_PRIMARY_COLUMN_NAMES: [&str; 2] = ["geometry", "geography"];

/// The actual encoding of the geometry in the Parquet file.
///
/// In contrast to the _user-specified API_, which is just "WKB" or "Native", here we need to know
/// the actual written encoding type so that we can save that in the metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub enum GeoParquetColumnEncoding {
    /// Serialized Well-known Binary encoding
    WKB,
    /// Native Point encoding
    #[serde(rename = "point")]
    Point,
    /// Native LineString encoding
    #[serde(rename = "linestring")]
    LineString,
    /// Native Polygon encoding
    #[serde(rename = "polygon")]
    Polygon,
    /// Native MultiPoint encoding
    #[serde(rename = "multipoint")]
    MultiPoint,
    /// Native MultiLineString encoding
    #[serde(rename = "multilinestring")]
    MultiLineString,
    /// Native MultiPolygon encoding
    #[serde(rename = "multipolygon")]
    MultiPolygon,
}

impl GeoParquetColumnEncoding {
    /// Construct a new column encoding based on the user's desired encoding
    pub(crate) fn try_new(
        writer_encoding: GeoParquetWriterEncoding,
        data_type: &GeoArrowType,
    ) -> GeoArrowResult<Self> {
        let new_encoding = match writer_encoding {
            GeoParquetWriterEncoding::WKB => Self::WKB,
            GeoParquetWriterEncoding::GeoArrow => match data_type {
                GeoArrowType::Point(_) => Self::Point,
                GeoArrowType::LineString(_) => Self::LineString,
                GeoArrowType::Polygon(_) => Self::Polygon,
                GeoArrowType::MultiPoint(_) => Self::MultiPoint,
                GeoArrowType::MultiLineString(_) => Self::MultiLineString,
                GeoArrowType::MultiPolygon(_) => Self::MultiPolygon,
                dt => {
                    return Err(GeoArrowError::GeoParquet(format!(
                        "unsupported data type for native encoding: {dt:?}",
                    )));
                }
            },
        };
        Ok(new_encoding)
    }

    /// The geometry type a native encoding stores, or `None` for WKB.
    pub(crate) fn native_geometry_type(&self) -> Option<GeoParquetGeometryType> {
        match self {
            Self::WKB => None,
            Self::Point => Some(GeoParquetGeometryType::Point),
            Self::LineString => Some(GeoParquetGeometryType::LineString),
            Self::Polygon => Some(GeoParquetGeometryType::Polygon),
            Self::MultiPoint => Some(GeoParquetGeometryType::MultiPoint),
            Self::MultiLineString => Some(GeoParquetGeometryType::MultiLineString),
            Self::MultiPolygon => Some(GeoParquetGeometryType::MultiPolygon),
        }
    }
}

impl Display for GeoParquetColumnEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use GeoParquetColumnEncoding::*;
        match self {
            WKB => write!(f, "WKB"),
            Point => write!(f, "point"),
            LineString => write!(f, "linestring"),
            Polygon => write!(f, "polygon"),
            MultiPoint => write!(f, "multipoint"),
            MultiLineString => write!(f, "multilinestring"),
            MultiPolygon => write!(f, "multipolygon"),
        }
    }
}

/// Geometry types that are valid to write to GeoParquet 1.1
///
/// Note that this only defines the geometry type, not the dimension. The dimension is tracked
/// separately, and stored together in [`GeoParquetGeometryTypeAndDimension`]. On that type the
/// serde serialize and deserialize traits are implemented.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum GeoParquetGeometryType {
    /// Point geometry type
    Point,
    /// LineString geometry type
    LineString,
    /// Polygon geometry type
    Polygon,
    /// MultiPoint geometry type
    MultiPoint,
    /// MultiLineString geometry type
    MultiLineString,
    /// MultiPolygon geometry type
    MultiPolygon,
    /// GeometryCollection geometry type
    GeometryCollection,
}

impl FromStr for GeoParquetGeometryType {
    type Err = GeoArrowError;

    fn from_str(s: &str) -> GeoArrowResult<Self> {
        let out = match s {
            "Point" => Self::Point,
            "LineString" => Self::LineString,
            "Polygon" => Self::Polygon,
            "MultiPoint" => Self::MultiPoint,
            "MultiLineString" => Self::MultiLineString,
            "MultiPolygon" => Self::MultiPolygon,
            "GeometryCollection" => Self::GeometryCollection,
            other => {
                return Err(GeoArrowError::GeoParquet(format!(
                    "Unknown value for geometry_type: {other}"
                )));
            }
        };
        Ok(out)
    }
}

impl Display for GeoParquetGeometryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl GeoParquetGeometryType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Point => "Point",
            Self::LineString => "LineString",
            Self::Polygon => "Polygon",
            Self::MultiPoint => "MultiPoint",
            Self::MultiLineString => "MultiLineString",
            Self::MultiPolygon => "MultiPolygon",
            Self::GeometryCollection => "GeometryCollection",
        }
    }

    pub(crate) fn from_geometry_trait(geometry: &impl GeometryTrait) -> Self {
        match geometry.as_type() {
            geo_traits::GeometryType::Point(_) => Self::Point,
            geo_traits::GeometryType::LineString(_) => Self::LineString,
            geo_traits::GeometryType::Polygon(_) => Self::Polygon,
            geo_traits::GeometryType::MultiPoint(_) => Self::MultiPoint,
            geo_traits::GeometryType::MultiLineString(_) => Self::MultiLineString,
            geo_traits::GeometryType::MultiPolygon(_) => Self::MultiPolygon,
            geo_traits::GeometryType::GeometryCollection(_) => Self::GeometryCollection,
            _ => panic!("Unsupported geometry type"),
        }
    }
}

/// Geometry type and dimension
///
/// Note: we use [`SerializeDisplay`] and [`DeserializeFromStr`] for serde because the GeoParquet
/// spec says this concept is a single string with the dimension stored as a suffix.
/// <https://docs.rs/serde_with/3.12.0/serde_with/struct.DisplayFromStr.html>
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, SerializeDisplay, DeserializeFromStr)]
pub struct GeoParquetGeometryTypeAndDimension {
    geometry_type: GeoParquetGeometryType,
    dimension: Dimension,
}

impl GeoParquetGeometryTypeAndDimension {
    /// Create a new `GeoParquetGeometryTypeAndDimension`
    pub fn new(geometry_type: GeoParquetGeometryType, dimension: Dimension) -> Self {
        Self {
            geometry_type,
            dimension,
        }
    }

    /// Get the geometry type
    pub fn geometry_type(&self) -> GeoParquetGeometryType {
        self.geometry_type
    }

    /// Get the dimension
    pub fn dimension(&self) -> Dimension {
        self.dimension
    }

    /// Convert to a [`GeoArrowType`] based on the geometry type and dimension
    pub(crate) fn to_data_type(
        self,
        coord_type: CoordType,
        metadata: Arc<Metadata>,
    ) -> GeoArrowType {
        match self.geometry_type {
            GeoParquetGeometryType::Point => GeoArrowType::Point(
                PointType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
            GeoParquetGeometryType::LineString => GeoArrowType::LineString(
                LineStringType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
            GeoParquetGeometryType::Polygon => GeoArrowType::Polygon(
                PolygonType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
            GeoParquetGeometryType::MultiPoint => GeoArrowType::MultiPoint(
                MultiPointType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
            GeoParquetGeometryType::MultiLineString => GeoArrowType::MultiLineString(
                MultiLineStringType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
            GeoParquetGeometryType::MultiPolygon => GeoArrowType::MultiPolygon(
                MultiPolygonType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
            GeoParquetGeometryType::GeometryCollection => GeoArrowType::GeometryCollection(
                GeometryCollectionType::new(self.dimension, metadata).with_coord_type(coord_type),
            ),
        }
    }

    pub(crate) fn from_type_id(type_id: i8) -> Self {
        let dimension = match type_id / 10 {
            0 => Dimension::XY,
            1 => Dimension::XYZ,
            2 => Dimension::XYM,
            3 => Dimension::XYZM,
            _ => panic!("unsupported type_id: {type_id}"),
        };
        let geometry_type = match type_id % 10 {
            1 => GeoParquetGeometryType::Point,
            2 => GeoParquetGeometryType::LineString,
            3 => GeoParquetGeometryType::Polygon,
            4 => GeoParquetGeometryType::MultiPoint,
            5 => GeoParquetGeometryType::MultiLineString,
            6 => GeoParquetGeometryType::MultiPolygon,
            7 => GeoParquetGeometryType::GeometryCollection,
            _ => panic!("unsupported type_id: {type_id}"),
        };
        Self {
            geometry_type,
            dimension,
        }
    }
}

impl FromStr for GeoParquetGeometryTypeAndDimension {
    type Err = GeoArrowError;

    fn from_str(s: &str) -> GeoArrowResult<Self> {
        let (geometry_type, dimension) = if let Some((geom_type_str, dim_str)) = s.split_once(' ') {
            let dimension = match dim_str {
                "Z" => Dimension::XYZ,
                "M" => Dimension::XYM,
                "ZM" => Dimension::XYZM,
                _ => {
                    return Err(GeoArrowError::GeoParquet(format!(
                        "Unknown dimension suffix: {dim_str}"
                    )));
                }
            };
            (GeoParquetGeometryType::from_str(geom_type_str)?, dimension)
        } else {
            (GeoParquetGeometryType::from_str(s)?, Dimension::XY)
        };
        Ok(Self {
            geometry_type,
            dimension,
        })
    }
}

impl Display for GeoParquetGeometryTypeAndDimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dimension_suffix = match self.dimension {
            Dimension::XY => "",
            Dimension::XYZ => " Z",
            Dimension::XYM => " M",
            Dimension::XYZM => " ZM",
        };
        write!(f, "{}{}", self.geometry_type, dimension_suffix,)
    }
}

/// Bounding-box covering
///
/// Including a per-row bounding box can be useful for accelerating spatial queries by allowing
/// consumers to inspect row group and page index bounding box summary statistics. Furthermore a
/// bounding box may be used to avoid complex spatial operations by first checking for bounding box
/// overlaps. This field captures the column name and fields containing the bounding box of the
/// geometry for every row.
///
/// The format of the bbox encoding is
/// ```json
/// {
///     "xmin": ["column_name", "xmin"],
///     "ymin": ["column_name", "ymin"],
///     "xmax": ["column_name", "xmax"],
///     "ymax": ["column_name", "ymax"]
/// }
/// ```
///
/// The arrays represent Parquet schema paths for nested groups. In this example, column_name is a
/// Parquet group with fields xmin, ymin, xmax, ymax. The value in column_name MUST exist in the
/// Parquet file and meet the criteria in the Bounding Box Column definition. In order to constrain
/// this value to a single bounding group field, the second item in each element MUST be xmin,
/// ymin, etc. All values MUST use the same column name.
///
/// The value specified in this field should not be confused with the top-level bbox field which
/// contains the single bounding box of this geometry over the whole GeoParquet file.
///
/// Note: This technique to use the bounding box to improve spatial queries does not apply to
/// geometries that cross the antimeridian. Such geometries are unsupported by this method.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoParquetBboxCovering {
    /// The path in the Parquet schema of the column that contains the xmin
    pub xmin: Vec<String>,

    /// The path in the Parquet schema of the column that contains the ymin
    pub ymin: Vec<String>,

    /// The path in the Parquet schema of the column that contains the zmin
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zmin: Option<Vec<String>>,

    /// The path in the Parquet schema of the column that contains the xmax
    pub xmax: Vec<String>,

    /// The path in the Parquet schema of the column that contains the ymax
    pub ymax: Vec<String>,

    /// The path in the Parquet schema of the column that contains the zmax
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zmax: Option<Vec<String>>,
}

impl GeoParquetBboxCovering {
    /// Infer a bbox covering from a native geoarrow encoding
    ///
    /// Note: for now this infers 2D boxes only
    pub(crate) fn infer_from_native(
        column_name: &str,
        column_metadata: &GeoParquetColumnMetadata,
    ) -> Option<Self> {
        use GeoParquetColumnEncoding::*;
        let (x, y) = match column_metadata.encoding {
            WKB => return None,
            Point => {
                let x = vec![column_name.to_string(), "x".to_string()];
                let y = vec![column_name.to_string(), "y".to_string()];
                (x, y)
            }
            LineString => {
                let x = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "x".to_string(),
                ];
                let y = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "y".to_string(),
                ];
                (x, y)
            }
            Polygon => {
                let x = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "x".to_string(),
                ];
                let y = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "y".to_string(),
                ];
                (x, y)
            }
            MultiPoint => {
                let x = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "x".to_string(),
                ];
                let y = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "y".to_string(),
                ];
                (x, y)
            }
            MultiLineString => {
                let x = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "x".to_string(),
                ];
                let y = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "y".to_string(),
                ];
                (x, y)
            }
            MultiPolygon => {
                let x = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "x".to_string(),
                ];
                let y = vec![
                    column_name.to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "list".to_string(),
                    "element".to_string(),
                    "y".to_string(),
                ];
                (x, y)
            }
        };

        Some(Self {
            xmin: x.clone(),
            ymin: y.clone(),
            zmin: None,
            xmax: x,
            ymax: y,
            zmax: None,
        })
    }
}

/// Object containing bounding box column names to help accelerate spatial data retrieval
///
/// The covering field specifies optional simplified representations of each geometry. The keys of
/// the "covering" object MUST be a supported encoding. Currently the only supported encoding is
/// "bbox" which specifies the names of bounding box columns
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoParquetCovering {
    /// Bounding-box covering
    pub bbox: GeoParquetBboxCovering,
}

/// Top-level GeoParquet file metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoParquetMetadata {
    /// The version identifier for the GeoParquet specification.
    pub version: String,

    /// The name of the "primary" geometry column. In cases where a GeoParquet file contains
    /// multiple geometry columns, the primary geometry may be used by default in geospatial
    /// operations.
    pub primary_column: String,

    /// Metadata about geometry columns. Each key is the name of a geometry column in the table.
    pub columns: HashMap<String, GeoParquetColumnMetadata>,
}

impl GeoParquetMetadata {
    /// Construct a [`GeoParquetMetadata`] from Parquet [`FileMetaData`]
    ///
    /// Returns `None` if the file does not contain GeoParquet metadata (i.e. there is no `geo`
    /// key). Returns `Some(Err(...))` if the metadata is present but cannot be parsed.
    pub fn from_parquet_meta(metadata: &FileMetaData) -> Option<GeoArrowResult<Self>> {
        let kv_metadata = metadata.key_value_metadata();

        if let Some(metadata) = kv_metadata {
            for kv in metadata {
                if kv.key == "geo" {
                    return kv.value.as_ref().map(|value| {
                        serde_json::from_str(value)
                            .map_err(|err| GeoArrowError::GeoParquet(err.to_string()))
                    });
                }
            }
        }

        None
    }

    /// The declared specification version, or `None` for unknown version strings.
    pub fn known_version(&self) -> Option<GeoParquetVersion> {
        GeoParquetVersion::from_metadata_string(&self.version)
    }

    /// Synthesize metadata from the Parquet GEOMETRY and GEOGRAPHY logical types.
    ///
    /// GeoParquet 2.0 expects readers to read files that carry only these types and no `geo`
    /// key. Each top-level geometry-typed column becomes a WKB column with unknown geometry
    /// types. Returns `None` when no such column exists.
    ///
    /// GeoParquet 2.0 is at release candidate 2.0.0-rc.1; this behavior can change until the
    /// specification is final.
    pub fn from_logical_types(
        parquet_schema: &SchemaDescriptor,
        key_value_metadata: Option<&Vec<KeyValue>>,
    ) -> Option<GeoArrowResult<Self>> {
        let mut columns: HashMap<String, GeoParquetColumnMetadata> = HashMap::new();
        for field in parquet_schema.root_schema().get_fields() {
            let Some(logical_type) = field.get_basic_info().logical_type_ref() else {
                continue;
            };
            let (crs, edges) = match logical_type {
                LogicalType::Geometry(geometry) => (geometry.crs.clone(), None),
                LogicalType::Geography(geography) => {
                    let edges = match geography.algorithm().map(edges_name_for_algorithm) {
                        Some(Ok(name)) => Some(name),
                        Some(Err(err)) => return Some(Err(err)),
                        None => None,
                    };
                    (geography.crs.clone(), edges)
                }
                _ => continue,
            };
            let column = match synthesized_column(crs.as_deref(), edges, key_value_metadata) {
                Ok(column) => column,
                Err(err) => return Some(Err(err)),
            };
            columns.insert(field.name().to_string(), column);
        }

        if columns.is_empty() {
            return None;
        }

        let primary_column = INFERRED_PRIMARY_COLUMN_NAMES
            .iter()
            .find(|name| columns.contains_key(**name))
            .map(|name| name.to_string())
            .unwrap_or_else(|| {
                let mut names: Vec<&String> = columns.keys().collect();
                names.sort();
                names[0].clone()
            });

        Some(Ok(Self {
            // Files carrying the geospatial logical types belong to the 2.0 ecosystem.
            version: GeoParquetVersion::V2_0.as_str().to_string(),
            primary_column,
            columns,
        }))
    }

    /// Merge another file's metadata into this one
    ///
    /// Expands each column's bbox, unions its geometry types, and carries over columns only
    /// `other` declares. Apart from the version string, which keeps the first-seen file's
    /// value, the merged column metadata is the same in any file order.
    pub fn try_update(&mut self, other: &GeoParquetMetadata) -> GeoArrowResult<()> {
        self.try_compatible_with(other)?;
        for (column_name, column_meta) in self.columns.iter_mut() {
            let Some(other_column_meta) = other.columns.get(column_name.as_str()) else {
                continue;
            };

            // Writers (e.g. GeoPandas) record per-file geometry_types, so two files of one
            // dataset legitimately differ; the dataset-level list is their union.
            column_meta
                .geometry_types
                .extend(other_column_meta.geometry_types.iter().cloned());

            match (column_meta.bbox.as_mut(), &other_column_meta.bbox) {
                (Some(bbox), Some(other_bbox)) => {
                    bbox.expand_to_include(other_bbox).map_err(|_| {
                        GeoArrowError::GeoParquet(format!(
                            "Different bbox dimensions for column {column_name}",
                        ))
                    })?;
                }
                (None, Some(other_bbox)) => {
                    column_meta.bbox = Some(other_bbox.clone());
                }
                // If the RHS doesn't have a bbox, we don't need to update
                (_, None) => {}
            }
        }

        // Carry over columns declared by only one file; the dataset reader's Arrow schema
        // check guarantees the column exists physically in every file.
        for (column_name, other_column_meta) in other.columns.iter() {
            if !self.columns.contains_key(column_name) {
                self.columns
                    .insert(column_name.clone(), other_column_meta.clone());
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
    pub fn try_compatible_with(&self, other: &GeoParquetMetadata) -> GeoArrowResult<()> {
        // The version string is deliberately not compared: a dataset written across a spec
        // transition (1.1 files next to 2.0 files) merges on the per-column metadata, and the
        // merged result keeps the first-seen version string.
        if self.primary_column.as_str() != other.primary_column.as_str() {
            return Err(GeoArrowError::GeoParquet(
                "Different GeoParquet primary columns".to_string(),
            ));
        }

        // Columns declared by only one side, and differing geometry_types, merge in
        // try_update instead of failing here.
        for (key, left) in self.columns.iter() {
            let Some(right) = other.columns.get(key) else {
                continue;
            };

            if left.encoding != right.encoding {
                return Err(GeoArrowError::GeoParquet(format!(
                    "Different GeoParquet encodings for column {key}",
                )));
            }

            if let (Some(left_bbox), Some(right_bbox)) = (&left.bbox, &right.bbox)
                && std::mem::discriminant(left_bbox) != std::mem::discriminant(right_bbox)
            {
                return Err(GeoArrowError::GeoParquet(format!(
                    "Different bbox dimensions for column {key}",
                )));
            }

            match (left.crs.as_ref(), right.crs.as_ref()) {
                (Some(left_crs), Some(right_crs)) => {
                    if left_crs != right_crs {
                        return Err(GeoArrowError::GeoParquet(format!(
                            "Different GeoParquet CRS for column {key}",
                        )));
                    }
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(GeoArrowError::GeoParquet(format!(
                        "Different GeoParquet CRS for column {key}",
                    )));
                }
                (None, None) => (),
            }
        }

        Ok(())
    }

    /// Access the geometry column with the given name.
    ///
    /// Returns (geometry column name, geometry column metadata)
    pub(crate) fn geometry_column<'a>(
        &'a self,
        column_name: Option<&'a str>,
    ) -> GeoArrowResult<(&'a str, &'a GeoParquetColumnMetadata)> {
        if let Some(column_name) = column_name {
            let column_meta = self
                .columns
                .get(column_name)
                .ok_or(GeoArrowError::GeoParquet(format!(
                    "Geometry column with name {column_name} not found in metadata"
                )))?;
            Ok((column_name, column_meta))
        } else {
            let column_meta =
                self.columns
                    .get(&self.primary_column)
                    .ok_or(GeoArrowError::GeoParquet(format!(
                        "Inferred primary geometry column with name {} not found in metadata",
                        self.primary_column
                    )))?;
            Ok((&self.primary_column, column_meta))
        }
    }
}

/// A GeoParquet specification version with version-specific writer rules.
///
/// The reader does not gate on the version: it stays layout-driven, and an unknown version
/// string reads like any other file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum GeoParquetVersion {
    /// GeoParquet 1.0.0
    V1_0,
    /// GeoParquet 1.1.0
    #[default]
    V1_1,
    /// GeoParquet 2.0.0
    ///
    /// The 2.0 specification is at release candidate 2.0.0-rc.1; this crate's 2.0 behavior can
    /// change until it is final.
    V2_0,
}

impl GeoParquetVersion {
    /// The version string written to the `geo` metadata key.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V1_0 => "1.0.0",
            Self::V1_1 => "1.1.0",
            Self::V2_0 => "2.0.0",
        }
    }

    /// Interpret a file's version string, or `None` for unknown versions.
    ///
    /// Pre-release strings count as their release: `"1.0.0-beta.1"` is 1.0, `"2.0.0-rc.1"`
    /// and the draft string `"2.0-dev"` are 2.0.
    pub fn from_metadata_string(version: &str) -> Option<Self> {
        match version {
            v if v == "1.0.0" || v.starts_with("1.0.0-") => Some(Self::V1_0),
            "1.1.0" => Some(Self::V1_1),
            v if v == "2.0.0" || v.starts_with("2.0.0-") || v == "2.0-dev" => Some(Self::V2_0),
            _ => None,
        }
    }

    /// Native (GeoArrow) encodings exist only in GeoParquet 1.1; 1.0 and 2.0 are WKB-only.
    pub fn supports_native_encoding(&self) -> bool {
        matches!(self, Self::V1_1)
    }

    /// The bbox covering requires 1.1 or later.
    ///
    /// The covering is out of the 2.0 release candidate text, but the 1.1 form stays valid as
    /// an extension and reinstatement is under discussion (opengeospatial/geoparquet#297).
    /// This crate writes a covering into 2.0 output only on explicit request.
    pub fn supports_covering(&self) -> bool {
        matches!(self, Self::V1_1 | Self::V2_0)
    }

    /// M coordinates arrive with GeoParquet 2.0; 1.x forbids them.
    pub fn supports_m_dimension(&self) -> bool {
        matches!(self, Self::V2_0)
    }
}

/// A geo metadata bounding box, in the spec's flat-array form.
///
/// A 6-element bbox is always XYZ: GeoParquet has no 6-element XYM form
/// (opengeospatial/geoparquet#300). The 8-element XYZM form arrives with GeoParquet 2.0.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<f64>", into = "Vec<f64>")]
pub enum GeoParquetBbox {
    /// `[xmin, ymin, xmax, ymax]`
    Xy([f64; 4]),
    /// `[xmin, ymin, zmin, xmax, ymax, zmax]`
    Xyz([f64; 6]),
    /// `[xmin, ymin, zmin, mmin, xmax, ymax, zmax, mmax]`
    Xyzm([f64; 8]),
}

impl GeoParquetBbox {
    /// The flat array: all minimums, then all maximums.
    pub fn as_slice(&self) -> &[f64] {
        match self {
            Self::Xy(v) => v,
            Self::Xyz(v) => v,
            Self::Xyzm(v) => v,
        }
    }

    /// Expand these bounds to also cover `other`.
    ///
    /// Errors if the two bboxes have different dimensions.
    pub fn expand_to_include(&mut self, other: &GeoParquetBbox) -> GeoArrowResult<()> {
        let (this, other) = match (self, other) {
            (Self::Xy(a), Self::Xy(b)) => (a.as_mut_slice(), b.as_slice()),
            (Self::Xyz(a), Self::Xyz(b)) => (a.as_mut_slice(), b.as_slice()),
            (Self::Xyzm(a), Self::Xyzm(b)) => (a.as_mut_slice(), b.as_slice()),
            _ => {
                return Err(GeoArrowError::GeoParquet(
                    "Different bbox dimensions".to_string(),
                ));
            }
        };
        let half = this.len() / 2;
        for (bound, other_bound) in this[..half].iter_mut().zip(&other[..half]) {
            *bound = bound.min(*other_bound);
        }
        for (bound, other_bound) in this[half..].iter_mut().zip(&other[half..]) {
            *bound = bound.max(*other_bound);
        }
        Ok(())
    }
}

impl TryFrom<Vec<f64>> for GeoParquetBbox {
    type Error = String;

    fn try_from(value: Vec<f64>) -> Result<Self, Self::Error> {
        match value.len() {
            4 => Ok(Self::Xy(value.try_into().unwrap())),
            6 => Ok(Self::Xyz(value.try_into().unwrap())),
            8 => Ok(Self::Xyzm(value.try_into().unwrap())),
            len => Err(format!("Invalid bbox length {len}: expected 4, 6, or 8")),
        }
    }
}

impl From<GeoParquetBbox> for Vec<f64> {
    fn from(value: GeoParquetBbox) -> Self {
        value.as_slice().to_vec()
    }
}

/// GeoParquet column metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoParquetColumnMetadata {
    /// Name of the geometry encoding format. As of GeoParquet 1.1, `"WKB"`, `"point"`,
    /// `"linestring"`, `"polygon"`, `"multipoint"`, `"multilinestring"`, and `"multipolygon"` are
    /// supported.
    pub encoding: GeoParquetColumnEncoding,

    /// The geometry types of all geometries, or an empty array if they are not known.
    ///
    /// This field captures the geometry types of the geometries in the column, when known.
    /// Accepted geometry types are: `"Point"`, `"LineString"`, `"Polygon"`, `"MultiPoint"`,
    /// `"MultiLineString"`, `"MultiPolygon"`, `"GeometryCollection"`.
    ///
    /// In addition, the following rules are used:
    ///
    /// - In case of 3D geometries, a `" Z"` suffix gets added (e.g. `["Point Z"]`).
    /// - A list of multiple values indicates that multiple geometry types are present (e.g.
    ///   `["Polygon", "MultiPolygon"]`).
    /// - An empty array explicitly signals that the geometry types are not known.
    /// - The geometry types in the list must be unique (e.g. `["Point", "Point"]` is not valid).
    ///
    /// It is expected that this field is strictly correct. For example, if having both polygons
    /// and multipolygons, it is not sufficient to specify `["MultiPolygon"]`, but it is expected
    /// to specify `["Polygon", "MultiPolygon"]`. Or if having 3D points, it is not sufficient to
    /// specify `["Point"]`, but it is expected to list `["Point Z"]`.
    pub geometry_types: HashSet<GeoParquetGeometryTypeAndDimension>,

    /// [PROJJSON](https://proj.org/specifications/projjson.html) object representing the
    /// Coordinate Reference System (CRS) of the geometry. If the field is not provided, the
    /// default CRS is [OGC:CRS84](https://www.opengis.net/def/crs/OGC/1.3/CRS84), which means the
    /// data in this column must be stored in longitude, latitude based on the WGS84 datum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crs: Option<Value>,

    /// Winding order of exterior ring of polygons. If present must be `"counterclockwise"`;
    /// interior rings are wound in opposite order. If absent, no assertions are made regarding the
    /// winding order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orientation: Option<String>,

    /// Name of the coordinate system for the edges. Must be one of `"planar"` or `"spherical"`.
    /// The default value is `"planar"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edges: Option<String>,

    /// Bounding Box of the geometries in the file, formatted according to RFC 7946, section 5.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<GeoParquetBbox>,

    /// Coordinate epoch in case of a dynamic CRS, expressed as a decimal year.
    ///
    /// In a dynamic CRS, coordinates of a point on the surface of the Earth may change with time.
    /// To be unambiguous, the coordinates must always be qualified with the epoch at which they
    /// are valid.
    ///
    /// The optional epoch field allows to specify this in case the crs field defines a dynamic
    /// CRS. The coordinate epoch is expressed as a decimal year (e.g. `2021.47`). Currently, this
    /// specification only supports an epoch per column (and not per geometry).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<f64>,

    /// Object containing bounding box column names to help accelerate spatial data retrieval
    #[serde(skip_serializing_if = "Option::is_none")]
    pub covering: Option<GeoParquetCovering>,

    /// Whether or not to use large i64 offsets or smaller i32 ones
    /// when writing this column as WKB
    // Skip serde since this is not a field of the GeoParquet spec
    // but it is used internally when determining how to encode the column
    #[serde(skip)]
    pub large_offsets: bool,
}

impl GeoParquetColumnMetadata {
    /// Get the bounding box covering for this geometry column.
    ///
    /// If the geometry column described by this [`GeoParquetColumnMetadata`] has associated
    /// bounding box columns, those will be returned. If it is a native encoding its covering will
    /// be inferred from the native columns. If it is a WKB encoding without associated bounding
    /// box columns, `None` will be returned.
    pub(crate) fn bbox_covering(
        &self,
        geometry_column_name: &str,
    ) -> Option<GeoParquetBboxCovering> {
        if let Some(covering) = &self.covering {
            Some(covering.bbox.clone())
        } else {
            GeoParquetBboxCovering::infer_from_native(geometry_column_name, self)
        }
    }
}

impl From<GeoParquetColumnMetadata> for Metadata {
    fn from(value: GeoParquetColumnMetadata) -> Self {
        let edges = value.edges.as_deref().and_then(edges_from_name);
        let crs = match value.crs {
            // A JSON string appears only in metadata synthesized from the Parquet logical
            // types, where it carries an authority code.
            Some(Value::String(authority_code)) => Crs::from_authority_code(authority_code),
            Some(projjson) => Crs::from_projjson(projjson),
            None => Crs::default(),
        };
        Metadata::new(crs, edges)
    }
}

/// The `geo` metadata `edges` names, aligned with the Parquet edge interpolation algorithms.
fn edges_from_name(name: &str) -> Option<Edges> {
    match name {
        "spherical" => Some(Edges::Spherical),
        "vincenty" => Some(Edges::Vincenty),
        "thomas" => Some(Edges::Thomas),
        "andoyer" => Some(Edges::Andoyer),
        "karney" => Some(Edges::Karney),
        _ => None,
    }
}

fn edges_name_for_algorithm(algorithm: EdgeInterpolationAlgorithm) -> GeoArrowResult<&'static str> {
    match algorithm {
        EdgeInterpolationAlgorithm::SPHERICAL => Ok("spherical"),
        EdgeInterpolationAlgorithm::VINCENTY => Ok("vincenty"),
        EdgeInterpolationAlgorithm::THOMAS => Ok("thomas"),
        EdgeInterpolationAlgorithm::ANDOYER => Ok("andoyer"),
        EdgeInterpolationAlgorithm::KARNEY => Ok("karney"),
        // Reading an unknown algorithm as planar would silently change geometry semantics.
        other => Err(GeoArrowError::GeoParquet(format!(
            "Unknown edge interpolation algorithm: {other:?}"
        ))),
    }
}

fn synthesized_column(
    crs: Option<&str>,
    edges: Option<&str>,
    key_value_metadata: Option<&Vec<KeyValue>>,
) -> GeoArrowResult<GeoParquetColumnMetadata> {
    let mut column = serde_json::json!({
        "encoding": "WKB",
        // The empty list explicitly signals that the geometry types are not known.
        "geometry_types": [],
    });
    if let Some(crs_value) = parquet_crs_to_geo_crs(crs, key_value_metadata)? {
        column["crs"] = crs_value;
    }
    if let Some(edges) = edges {
        column["edges"] = Value::String(edges.to_string());
    }
    serde_json::from_value(column).map_err(|err| GeoArrowError::GeoParquet(err.to_string()))
}

/// Interpret the Parquet logical-type `crs` property.
///
/// Four forms: inline PROJJSON, `projjson:<key>` naming a file metadata key that holds
/// PROJJSON, `srid:<identifier>`, and `<authority>:<code>`. An `srid:` identifier is not
/// resolvable without a CRS database and maps to no CRS; an authority code stays a JSON
/// string for the [`Metadata`] conversion.
fn parquet_crs_to_geo_crs(
    crs: Option<&str>,
    key_value_metadata: Option<&Vec<KeyValue>>,
) -> GeoArrowResult<Option<Value>> {
    // An absent crs property means OGC:CRS84, which is also the `geo` metadata default.
    let Some(crs) = crs else { return Ok(None) };
    let crs = crs.trim();
    if crs.starts_with('{') {
        let value = serde_json::from_str(crs).map_err(|err| {
            GeoArrowError::GeoParquet(format!("Invalid PROJJSON in Parquet crs property: {err}"))
        })?;
        return Ok(Some(value));
    }
    if let Some(key) = crs.strip_prefix("projjson:") {
        let value = key_value_metadata
            .and_then(|kvs| kvs.iter().find(|kv| kv.key == key))
            .and_then(|kv| kv.value.as_deref())
            .ok_or_else(|| {
                GeoArrowError::GeoParquet(format!(
                    "Parquet crs property references missing file metadata key {key}"
                ))
            })?;
        let value = serde_json::from_str(value).map_err(|err| {
            GeoArrowError::GeoParquet(format!(
                "Invalid PROJJSON under file metadata key {key}: {err}"
            ))
        })?;
        return Ok(Some(value));
    }
    if crs.strip_prefix("srid:").is_some() {
        return Ok(None);
    }
    Ok(Some(Value::String(crs.to_string())))
}

// TODO: deduplicate with `resolve_types` in `downcast.rs`
pub(crate) fn infer_geo_data_type(
    geometry_types: &HashSet<GeoParquetGeometryTypeAndDimension>,
    coord_type: CoordType,
    metadata: Arc<Metadata>,
) -> GeoArrowResult<Option<GeoArrowType>> {
    use GeoParquetGeometryType::*;

    let fallback_geometry_type =
        GeoArrowType::Geometry(GeometryType::new(metadata.clone()).with_coord_type(coord_type));

    match geometry_types.len() {
        // TODO: for unknown geometry type, should we leave it as WKB?
        0 => Ok(None),
        1 => Ok(Some(
            geometry_types
                .iter()
                .next()
                .unwrap()
                .to_data_type(coord_type, metadata),
        )),
        _ => {
            // If there are multiple dimensions, we can't cast to a single primitive geometry array
            // type.
            let dimensions = geometry_types
                .iter()
                .map(|t| t.dimension)
                .collect::<HashSet<_>>();
            if dimensions.len() > 1 {
                return Ok(Some(fallback_geometry_type));
            }

            let single_dimension = dimensions.into_iter().next().unwrap();

            let geometry_types = geometry_types
                .iter()
                .map(|t| t.geometry_type())
                .collect::<HashSet<_>>();

            // Check if we can cast to MultiPoint
            let mut point_count = 0;
            if geometry_types.contains(&Point) {
                point_count += 1;
            }
            if geometry_types.contains(&MultiPoint) {
                point_count += 1;
            }

            if geometry_types.len() == point_count {
                return Ok(Some(GeoArrowType::MultiPoint(
                    MultiPointType::new(single_dimension, metadata).with_coord_type(coord_type),
                )));
            }

            // Check if we can cast to MultiLineString
            let mut linestring_count = 0;
            if geometry_types.contains(&LineString) {
                linestring_count += 1;
            }
            if geometry_types.contains(&MultiLineString) {
                linestring_count += 1;
            }

            if geometry_types.len() == linestring_count {
                return Ok(Some(GeoArrowType::MultiLineString(
                    MultiLineStringType::new(single_dimension, metadata)
                        .with_coord_type(coord_type),
                )));
            }

            // Check if we can cast to MultiPolygon
            let mut polygon_count = 0;
            if geometry_types.contains(&Polygon) {
                polygon_count += 1;
            }
            if geometry_types.contains(&MultiPolygon) {
                polygon_count += 1;
            }

            if geometry_types.len() == polygon_count {
                return Ok(Some(GeoArrowType::MultiPolygon(
                    MultiPolygonType::new(single_dimension, metadata).with_coord_type(coord_type),
                )));
            }

            Ok(Some(fallback_geometry_type))
        }
    }
}

#[cfg(test)]
mod test {
    use parquet::file::metadata::KeyValue;

    use super::*;

    // We want to ensure that extra keys in future GeoParquet versions do not break
    // By default, serde allows and ignores unknown keys
    #[test]
    fn extra_keys_in_column_metadata() {
        let s = r#"{
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "other_key": true
        }"#;
        let meta: GeoParquetColumnMetadata = serde_json::from_str(s).unwrap();
        assert_eq!(meta.encoding, GeoParquetColumnEncoding::WKB);
        assert_eq!(
            meta.geometry_types.iter().next().unwrap(),
            &GeoParquetGeometryTypeAndDimension::new(GeoParquetGeometryType::Point, Dimension::XY)
        );

        dbg!(&meta);
    }

    #[test]
    fn bbox_serde_validates_length_and_round_trips() {
        let err = serde_json::from_value::<GeoParquetMetadata>(serde_json::json!({
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {"encoding": "WKB", "geometry_types": [], "bbox": [1.0, 2.0, 3.0]}
            }
        }))
        .err()
        .unwrap();
        assert!(err.to_string().contains("bbox length"));

        let xyzm = serde_json::json!([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        let bbox: GeoParquetBbox = serde_json::from_value(xyzm.clone()).unwrap();
        assert_eq!(
            bbox,
            GeoParquetBbox::Xyzm([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        );
        assert_eq!(serde_json::to_value(&bbox).unwrap(), xyzm);
    }

    #[test]
    fn logical_types_synthesize_geo_metadata() {
        let descr = crate::test::geometry_schema_descr(LogicalType::geography(
            Some("EPSG:32633".to_string()),
            Some(EdgeInterpolationAlgorithm::KARNEY),
        ));
        let meta = GeoParquetMetadata::from_logical_types(&descr, None)
            .unwrap()
            .unwrap();
        assert_eq!(meta.version, "2.0.0");
        assert_eq!(meta.primary_column, "geometry");
        let column = &meta.columns["geometry"];
        assert!(matches!(column.encoding, GeoParquetColumnEncoding::WKB));
        assert!(column.geometry_types.is_empty());
        assert_eq!(column.edges.as_deref(), Some("karney"));

        let geoarrow_meta = Metadata::from(column.clone());
        assert_eq!(geoarrow_meta.edges(), Some(Edges::Karney));
        assert_eq!(
            geoarrow_meta.crs(),
            &Crs::from_authority_code("EPSG:32633".to_string())
        );

        let descr = crate::test::schema_descr("message schema { required binary name; }");
        assert!(GeoParquetMetadata::from_logical_types(&descr, None).is_none());
    }

    #[test]
    fn parquet_crs_property_forms() {
        let inline = parquet_crs_to_geo_crs(Some(r#"{"type": "GeographicCRS"}"#), None).unwrap();
        assert_eq!(inline, Some(serde_json::json!({"type": "GeographicCRS"})));

        let kv = vec![KeyValue::new(
            "my_crs".to_string(),
            r#"{"a": 1}"#.to_string(),
        )];
        let referenced = parquet_crs_to_geo_crs(Some("projjson:my_crs"), Some(&kv)).unwrap();
        assert_eq!(referenced, Some(serde_json::json!({"a": 1})));
        assert!(parquet_crs_to_geo_crs(Some("projjson:absent"), Some(&kv)).is_err());

        assert_eq!(parquet_crs_to_geo_crs(Some("srid:0"), None).unwrap(), None);
        assert_eq!(parquet_crs_to_geo_crs(None, None).unwrap(), None);
        assert_eq!(
            parquet_crs_to_geo_crs(Some("EPSG:4326"), None).unwrap(),
            Some(Value::String("EPSG:4326".to_string()))
        );
    }

    #[test]
    fn version_strings_map_to_known_versions() {
        use GeoParquetVersion::*;
        for (string, expected) in [
            ("1.0.0", Some(V1_0)),
            ("1.0.0-beta.1", Some(V1_0)),
            ("1.1.0", Some(V1_1)),
            ("2.0.0", Some(V2_0)),
            ("2.0.0-rc.1", Some(V2_0)),
            ("2.0-dev", Some(V2_0)),
            ("0.4.0", None),
            ("3.0.0", None),
        ] {
            assert_eq!(
                GeoParquetVersion::from_metadata_string(string),
                expected,
                "{string}"
            );
        }
    }

    #[test]
    fn try_update_is_order_independent() {
        let meta_a: GeoParquetMetadata = serde_json::from_value(serde_json::json!({
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "geometry_types": ["MultiPolygon"],
                    "bbox": [0.0, 0.0, 1.0, 1.0]
                }
            }
        }))
        .unwrap();
        let meta_b: GeoParquetMetadata = serde_json::from_value(serde_json::json!({
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "geometry_types": ["Polygon", "MultiPolygon"],
                    "bbox": [-1.0, 0.5, 2.0, 0.75]
                },
                "geom2": {"encoding": "WKB", "geometry_types": ["Point"]}
            }
        }))
        .unwrap();

        let mut ab = meta_a.clone();
        ab.try_update(&meta_b).unwrap();
        let mut ba = meta_b.clone();
        ba.try_update(&meta_a).unwrap();

        for merged in [&ab, &ba] {
            let geom = &merged.columns["geom"];
            assert_eq!(geom.geometry_types.len(), 2);
            assert_eq!(geom.bbox, Some(GeoParquetBbox::Xy([-1.0, 0.0, 2.0, 1.0])));
            assert!(merged.columns.contains_key("geom2"));
        }
        assert_eq!(
            ab.columns["geom"].geometry_types,
            ba.columns["geom"].geometry_types
        );
    }

    #[test]
    fn bbox_expand_to_include() {
        let mut bbox = GeoParquetBbox::Xyz([0.0, 0.0, 0.0, 1.0, 1.0, 1.0]);
        bbox.expand_to_include(&GeoParquetBbox::Xyz([-1.0, 0.5, -2.0, 0.5, 3.0, 4.0]))
            .unwrap();
        assert_eq!(bbox, GeoParquetBbox::Xyz([-1.0, 0.0, -2.0, 1.0, 3.0, 4.0]));

        let err = bbox
            .expand_to_include(&GeoParquetBbox::Xy([0.0, 0.0, 1.0, 1.0]))
            .err()
            .unwrap();
        assert!(err.to_string().contains("bbox dimensions"));
    }
}
