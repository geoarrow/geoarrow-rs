use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use serde_json::Value;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::metadata::{ArrayMetadata, Edges};
use crate::array::CoordType;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{GeoParquetColumnMetadata, GeoParquetMetadata};
use crate::io::parquet::writer::options::{GeoParquetWriterEncoding, GeoParquetWriterOptions};

/// The actual encoding of the geometry in the Parquet file.
///
/// In contrast to the _user-specified API_, which is just "WKB" or "Native", here we need to know
/// the actual written encoding type so that we can save that in the metadata.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
pub enum GeoColumnEncoding {
    WKB,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl GeoColumnEncoding {
    pub fn try_new(
        writer_encoding: GeoParquetWriterEncoding,
        data_type: &GeoDataType,
    ) -> Result<Self> {
        let new_encoding = match writer_encoding {
            GeoParquetWriterEncoding::WKB => Self::WKB,
            GeoParquetWriterEncoding::Native => match data_type {
                GeoDataType::Point(_, Dimension::XY) => Self::Point,
                GeoDataType::LineString(_, Dimension::XY) => Self::LineString,
                GeoDataType::LargeLineString(_, Dimension::XY) => Self::LineString,
                GeoDataType::Polygon(_, Dimension::XY) => Self::Polygon,
                GeoDataType::LargePolygon(_, Dimension::XY) => Self::Polygon,
                GeoDataType::MultiPoint(_, Dimension::XY) => Self::MultiPoint,
                GeoDataType::LargeMultiPoint(_, Dimension::XY) => Self::MultiPoint,
                GeoDataType::MultiLineString(_, Dimension::XY) => Self::MultiLineString,
                GeoDataType::LargeMultiLineString(_, Dimension::XY) => Self::MultiLineString,
                GeoDataType::MultiPolygon(_, Dimension::XY) => Self::MultiPolygon,
                GeoDataType::LargeMultiPolygon(_, Dimension::XY) => Self::MultiPolygon,
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

impl Display for GeoColumnEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use GeoColumnEncoding::*;
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

/// Information for one geometry column being written to Parquet
pub struct ColumnInfo {
    /// The name of this geometry column
    pub name: String,

    /// The serialized encoding for this geometry column.
    pub encoding: GeoColumnEncoding,

    /// The set of string geometry types for this geometry column
    pub geometry_types: Vec<String>,

    /// The bounding box of this column.
    pub bbox: Option<BoundingRect>,

    /// The PROJJSON CRS for this geometry column.
    pub crs: Option<Value>,

    /// If present, instructs consumers that edges follow a spherical path rather than a planar
    /// one. If this value is omitted, edges will be interpreted as planar.
    pub edges: Option<Edges>,
}

impl ColumnInfo {
    pub fn try_new(
        name: String,
        writer_encoding: GeoParquetWriterEncoding,
        data_type: &GeoDataType,
        array_meta: ArrayMetadata,
    ) -> Result<Self> {
        let encoding = GeoColumnEncoding::try_new(writer_encoding, data_type)?;
        let geometry_types = get_geometry_types(data_type);
        Ok(Self {
            name,
            encoding,
            geometry_types,
            bbox: None,
            crs: array_meta.crs,
            edges: array_meta.edges,
        })
    }

    pub fn update_bbox(&mut self, new_bounds: &BoundingRect) {
        if let Some(existing_bounds) = self.bbox.as_mut() {
            existing_bounds.update(new_bounds)
        } else {
            self.bbox = Some(*new_bounds);
        }
    }

    /// Returns (column_name, column_metadata)
    pub fn finish(self) -> (String, GeoParquetColumnMetadata) {
        let edges = self.edges.map(|edges| match edges {
            Edges::Spherical => "spherical".to_string(),
        });
        let column_meta = GeoParquetColumnMetadata {
            encoding: self.encoding.to_string(),
            geometry_types: self.geometry_types,
            crs: self.crs,
            bbox: self
                .bbox
                .map(|bounds| vec![bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()]),
            edges,
            orientation: None,
            epoch: None,
            covering: None,
        };
        (self.name, column_meta)
    }
}

pub struct GeoParquetMetadataBuilder {
    pub output_schema: Arc<Schema>,
    pub primary_column: Option<String>,
    pub columns: HashMap<usize, ColumnInfo>,
}

impl GeoParquetMetadataBuilder {
    pub fn try_new(schema: &Schema, options: &GeoParquetWriterOptions) -> Result<Self> {
        let mut columns = HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(ext_name) = field.metadata().get("ARROW:extension:name") {
                if !ext_name.starts_with("geoarrow") {
                    continue;
                }

                let column_name = schema.field(col_idx).name().clone();

                let array_meta =
                    if let Some(ext_meta) = field.metadata().get("ARROW:extension:metadata") {
                        serde_json::from_str(ext_meta)?
                    } else {
                        ArrayMetadata::default()
                    };

                let geo_data_type = field.as_ref().try_into()?;

                let column_info =
                    ColumnInfo::try_new(column_name, options.encoding, &geo_data_type, array_meta)?;

                columns.insert(col_idx, column_info);
            }
        }

        let output_schema = create_output_schema(schema, &columns);
        Ok(Self {
            primary_column: None,
            columns,
            output_schema,
        })
    }

    #[allow(dead_code)]
    fn update_bounds(&mut self, bounds: &HashMap<usize, BoundingRect>) {
        for (column_idx, column_bounds) in bounds.iter() {
            self.columns
                .get_mut(column_idx)
                .unwrap()
                .update_bbox(column_bounds);
        }
    }

    pub fn finish(self) -> Option<GeoParquetMetadata> {
        let mut columns = HashMap::with_capacity(self.columns.len());
        for column_info in self.columns.into_values() {
            let (column_name, column_meta) = column_info.finish();
            columns.insert(column_name, column_meta);
        }

        if columns.is_empty() {
            None
        } else {
            Some(GeoParquetMetadata {
                version: "1.1.0".to_string(),
                primary_column: self
                    .primary_column
                    .unwrap_or_else(|| columns.keys().next().unwrap().clone()),
                columns,
            })
        }
    }
}

pub fn get_geometry_types(data_type: &GeoDataType) -> Vec<String> {
    match data_type {
        GeoDataType::Point(_, Dimension::XY) => vec!["Point".to_string()],
        GeoDataType::LineString(_, Dimension::XY)
        | GeoDataType::LargeLineString(_, Dimension::XY) => {
            vec!["LineString".to_string()]
        }
        GeoDataType::Polygon(_, Dimension::XY) | GeoDataType::LargePolygon(_, Dimension::XY) => {
            vec!["Polygon".to_string()]
        }
        GeoDataType::MultiPoint(_, Dimension::XY)
        | GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
            vec!["MultiPoint".to_string()]
        }
        GeoDataType::MultiLineString(_, Dimension::XY)
        | GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
            vec!["MultiLineString".to_string()]
        }
        GeoDataType::MultiPolygon(_, Dimension::XY)
        | GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
            vec!["MultiPolygon".to_string()]
        }
        GeoDataType::Mixed(_, Dimension::XY) | GeoDataType::LargeMixed(_, Dimension::XY) => {
            vec![]
            // let mut geom_types = HashSet::new();
            // arr.as_mixed_2d().chunks().iter().for_each(|chunk| {
            //     if chunk.has_points() {
            //         geom_types.insert("Point".to_string());
            //     }
            //     if chunk.has_line_string_2ds() {
            //         geom_types.insert("LineString".to_string());
            //     }
            //     if chunk.has_polygon_2ds() {
            //         geom_types.insert("Polygon".to_string());
            //     }
            //     if chunk.has_multi_point_2ds() {
            //         geom_types.insert("MultiPoint".to_string());
            //     }
            //     if chunk.has_multi_line_string_2ds() {
            //         geom_types.insert("MultiLineString".to_string());
            //     }
            //     if chunk.has_multi_polygon_2ds() {
            //         geom_types.insert("MultiPolygon".to_string());
            //     }
            // });
            // geom_types.into_iter().collect()
        }
        GeoDataType::GeometryCollection(_, Dimension::XY)
        | GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
            vec!["GeometryCollection".to_string()]
        }
        GeoDataType::WKB | GeoDataType::LargeWKB => vec![],
        GeoDataType::Rect(_) => unimplemented!(),
        _ => todo!("3d types"),
    }
}

fn create_output_schema(
    input_schema: &Schema,
    columns: &HashMap<usize, ColumnInfo>,
) -> Arc<Schema> {
    let mut fields = input_schema.fields().to_vec();
    for (column_idx, column_info) in columns.iter() {
        let existing_field = input_schema.field(*column_idx);
        let output_field = create_output_field(
            column_info.encoding,
            existing_field.name().clone(),
            // For now we always create nullable geometry fields
            true,
        );
        fields[*column_idx] = output_field.into();
    }

    Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

fn create_output_field(encoding: GeoColumnEncoding, name: String, nullable: bool) -> Field {
    match encoding {
        GeoColumnEncoding::WKB => GeoDataType::WKB.to_field(name, nullable),
        GeoColumnEncoding::Point => {
            GeoDataType::Point(CoordType::Separated, Dimension::XY).to_field(name, nullable)
        }
        GeoColumnEncoding::LineString => {
            GeoDataType::LineString(CoordType::Separated, Dimension::XY).to_field(name, nullable)
        }
        GeoColumnEncoding::Polygon => {
            GeoDataType::Polygon(CoordType::Separated, Dimension::XY).to_field(name, nullable)
        }
        GeoColumnEncoding::MultiPoint => {
            GeoDataType::MultiPoint(CoordType::Separated, Dimension::XY).to_field(name, nullable)
        }
        GeoColumnEncoding::MultiLineString => {
            GeoDataType::MultiLineString(CoordType::Separated, Dimension::XY)
                .to_field(name, nullable)
        }
        GeoColumnEncoding::MultiPolygon => {
            GeoDataType::MultiPolygon(CoordType::Separated, Dimension::XY).to_field(name, nullable)
        }
    }
}
