//! Strongly-typed structs corresponding to the metadata provided by the GeoParquet specification.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;

use arrow_schema::Schema;
use geoarrow_array::error::{GeoArrowError, Result};
use geoarrow_array::GeoArrowType;
use geoarrow_schema::{
    CoordType, Crs, Dimension, Edges, GeometryCollectionType, GeometryType, LineStringType,
    Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};
use parquet::file::metadata::FileMetaData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::GeoParquetWriterEncoding;

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
    ) -> Result<Self> {
        let new_encoding = match writer_encoding {
            GeoParquetWriterEncoding::WKB => Self::WKB,
            GeoParquetWriterEncoding::Native => match data_type {
                GeoArrowType::Point(_) => Self::Point,
                GeoArrowType::LineString(_) => Self::LineString,
                GeoArrowType::Polygon(_) => Self::Polygon,
                GeoArrowType::MultiPoint(_) => Self::MultiPoint,
                GeoArrowType::MultiLineString(_) => Self::MultiLineString,
                GeoArrowType::MultiPolygon(_) => Self::MultiPolygon,
                dt => {
                    return Err(GeoArrowError::General(format!(
                        "unsupported data type for native encoding: {:?}",
                        dt
                    )))
                }
            },
        };
        Ok(new_encoding)
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
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Point Z geometry type
    #[serde(rename = "Point Z")]
    PointZ,
    /// LineString Z geometry type
    #[serde(rename = "LineString Z")]
    LineStringZ,
    /// Polygon Z geometry type
    #[serde(rename = "Polygon Z")]
    PolygonZ,
    /// MultiPoint Z geometry type
    #[serde(rename = "MultiPoint Z")]
    MultiPointZ,
    /// MultiLineString Z geometry type
    #[serde(rename = "MultiLineString Z")]
    MultiLineStringZ,
    /// MultiPolygon Z geometry type
    #[serde(rename = "MultiPolygon Z")]
    MultiPolygonZ,
    /// GeometryCollection Z geometry type
    #[serde(rename = "GeometryCollection Z")]
    GeometryCollectionZ,
}

impl FromStr for GeoParquetGeometryType {
    type Err = GeoArrowError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let out = match s {
            "Point" => Self::Point,
            "LineString" => Self::LineString,
            "Polygon" => Self::Polygon,
            "MultiPoint" => Self::MultiPoint,
            "MultiLineString" => Self::MultiLineString,
            "MultiPolygon" => Self::MultiPolygon,
            "GeometryCollection" => Self::GeometryCollection,
            "Point Z" => Self::PointZ,
            "LineString Z" => Self::LineStringZ,
            "Polygon Z" => Self::PolygonZ,
            "MultiPoint Z" => Self::MultiPointZ,
            "MultiLineString Z" => Self::MultiLineStringZ,
            "MultiPolygon Z" => Self::MultiPolygonZ,
            "GeometryCollection Z" => Self::GeometryCollectionZ,
            other => {
                return Err(GeoArrowError::General(format!(
                    "Unknown value for geometry_type: {other}"
                )))
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
            Self::PointZ => "Point Z",
            Self::LineStringZ => "LineString Z",
            Self::PolygonZ => "Polygon Z",
            Self::MultiPointZ => "MultiPoint Z",
            Self::MultiLineStringZ => "MultiLineString Z",
            Self::MultiPolygonZ => "MultiPolygon Z",
            Self::GeometryCollectionZ => "GeometryCollection Z",
        }
    }

    #[allow(dead_code)]
    pub(crate) fn has_z(&self) -> bool {
        match self {
            Self::Point
            | Self::LineString
            | Self::Polygon
            | Self::MultiPoint
            | Self::MultiLineString
            | Self::MultiPolygon
            | Self::GeometryCollection => false,
            Self::PointZ
            | Self::LineStringZ
            | Self::PolygonZ
            | Self::MultiPointZ
            | Self::MultiLineStringZ
            | Self::MultiPolygonZ
            | Self::GeometryCollectionZ => true,
        }
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
    pub geometry_types: HashSet<GeoParquetGeometryType>,

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
    pub bbox: Option<Vec<f64>>,

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
}

impl GeoParquetMetadata {
    /// Construct a [`GeoParquetMetadata`] from Parquet [`FileMetaData`]
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

    /// Update a GeoParquetMetadata from another file's metadata
    ///
    /// This will expand the bounding box of each geometry column to include the bounding box
    /// defined in the other file's GeoParquet metadata
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

            if left.encoding != right.encoding {
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

    /// Get the bounding box covering for a geometry column
    ///
    /// If the desired column does not have covering metadata, if it is a native encoding its
    /// covering will be inferred.
    pub(crate) fn bbox_covering(
        &self,
        column_name: Option<&str>,
    ) -> Result<Option<GeoParquetBboxCovering>> {
        let column_name = column_name.unwrap_or(&self.primary_column);
        let column_meta = self
            .columns
            .get(column_name)
            .ok_or(GeoArrowError::General(format!(
                "Column name {column_name} not found in metadata"
            )))?;
        if let Some(covering) = &column_meta.covering {
            Ok(Some(covering.bbox.clone()))
        } else {
            let inferred_covering =
                GeoParquetBboxCovering::infer_from_native(column_name, column_meta);
            Ok(inferred_covering)
        }
    }
}

impl From<GeoParquetColumnMetadata> for Metadata {
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
        if let Some(crs) = value.crs {
            Metadata::new(Crs::from_projjson(crs), edges)
        } else {
            Metadata::default()
        }
    }
}

// TODO: deduplicate with `resolve_types` in `downcast.rs`
pub(crate) fn infer_geo_data_type(
    geometry_types: &HashSet<GeoParquetGeometryType>,
    coord_type: CoordType,
) -> Result<Option<GeoArrowType>> {
    use GeoParquetGeometryType::*;

    match geometry_types.len() {
        // TODO: for unknown geometry type, should we leave it as WKB?
        0 => Ok(None),
        1 => {
            Ok(Some(match *geometry_types.iter().next().unwrap() {
                Point => GeoArrowType::Point(PointType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                )),
                LineString => GeoArrowType::LineString(LineStringType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                )),
                Polygon => GeoArrowType::Polygon(PolygonType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                )),
                MultiPoint => GeoArrowType::MultiPoint(MultiPointType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                )),
                MultiLineString => GeoArrowType::MultiLineString(MultiLineStringType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                )),
                MultiPolygon => GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                )),
                GeometryCollection => GeoArrowType::GeometryCollection(
                    GeometryCollectionType::new(coord_type, Dimension::XY, Default::default()),
                ),
                PointZ => GeoArrowType::Point(PointType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                )),
                LineStringZ => GeoArrowType::LineString(LineStringType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                )),
                PolygonZ => GeoArrowType::Polygon(PolygonType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                )),
                MultiPointZ => GeoArrowType::MultiPoint(MultiPointType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                )),
                MultiLineStringZ => GeoArrowType::MultiLineString(MultiLineStringType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                )),
                MultiPolygonZ => GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                )),
                GeometryCollectionZ => GeoArrowType::GeometryCollection(
                    GeometryCollectionType::new(coord_type, Dimension::XYZ, Default::default()),
                ),
            }))
        }
        _ => {
            // Check if we can cast to MultiPoint
            let mut point_count = 0;
            if geometry_types.contains(&Point) {
                point_count += 1;
            }
            if geometry_types.contains(&MultiPoint) {
                point_count += 1;
            }

            if geometry_types.len() == point_count {
                return Ok(Some(GeoArrowType::MultiPoint(MultiPointType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                ))));
            }

            // Check if we can cast to MultiPointZ
            if geometry_types.contains(&PointZ) {
                point_count += 1;
            }
            if geometry_types.contains(&MultiPointZ) {
                point_count += 1;
            }

            if geometry_types.len() == point_count {
                return Ok(Some(GeoArrowType::MultiPoint(MultiPointType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                ))));
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
                    MultiLineStringType::new(coord_type, Dimension::XY, Default::default()),
                )));
            }

            // Check if we can cast to MultiLineStringZ
            if geometry_types.contains(&LineStringZ) {
                linestring_count += 1;
            }
            if geometry_types.contains(&MultiLineStringZ) {
                linestring_count += 1;
            }

            if geometry_types.len() == linestring_count {
                return Ok(Some(GeoArrowType::MultiLineString(
                    MultiLineStringType::new(coord_type, Dimension::XYZ, Default::default()),
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
                return Ok(Some(GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    coord_type,
                    Dimension::XY,
                    Default::default(),
                ))));
            }

            // Check if we can cast to MultiPolygonZ
            if geometry_types.contains(&PolygonZ) {
                polygon_count += 1;
            }
            if geometry_types.contains(&MultiPolygonZ) {
                polygon_count += 1;
            }

            if geometry_types.len() == polygon_count {
                return Ok(Some(GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    coord_type,
                    Dimension::XYZ,
                    Default::default(),
                ))));
            }

            Ok(Some(GeoArrowType::Geometry(GeometryType::new(
                coord_type,
                Default::default(),
            ))))
        }
    }
}

/// Find all geometry columns in the Arrow schema, constructing their NativeTypes
pub(crate) fn find_geoparquet_geom_columns(
    metadata: &FileMetaData,
    schema: &Schema,
    coord_type: CoordType,
) -> Result<Vec<(usize, Option<GeoArrowType>)>> {
    let meta = GeoParquetMetadata::from_parquet_meta(metadata)?;

    meta.columns
        .iter()
        .map(|(col_name, col_meta)| {
            let geometry_column_index = schema
                .fields()
                .iter()
                .position(|field| field.name().as_str() == col_name.as_str())
                .unwrap();
            Ok((
                geometry_column_index,
                infer_geo_data_type(&col_meta.geometry_types, coord_type)?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod test {
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
            &GeoParquetGeometryType::Point
        );

        dbg!(&meta);
    }
}
