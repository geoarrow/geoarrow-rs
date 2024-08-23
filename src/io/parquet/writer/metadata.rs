use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use serde_json::Value;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::metadata::{ArrayMetadata, Edges};
use crate::array::CoordType;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::Result;
use crate::io::parquet::metadata::{
    GeoParquetColumnEncoding, GeoParquetColumnMetadata, GeoParquetGeometryType, GeoParquetMetadata,
};
use crate::io::parquet::writer::options::{GeoParquetWriterEncoding, GeoParquetWriterOptions};

/// Information for one geometry column being written to Parquet
pub struct ColumnInfo {
    /// The name of this geometry column
    pub name: String,

    /// The serialized encoding for this geometry column.
    pub encoding: GeoParquetColumnEncoding,

    /// The set of string geometry types for this geometry column
    pub geometry_types: HashSet<GeoParquetGeometryType>,

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
        let encoding = GeoParquetColumnEncoding::try_new(writer_encoding, data_type)?;
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
        let bbox = if let Some(bbox) = self.bbox {
            if let (Some(minz), Some(maxz)) = (bbox.minz(), bbox.maxz()) {
                Some(vec![
                    bbox.minx(),
                    bbox.miny(),
                    minz,
                    bbox.maxx(),
                    bbox.maxy(),
                    maxz,
                ])
            } else {
                Some(vec![bbox.minx(), bbox.miny(), bbox.maxx(), bbox.maxy()])
            }
        } else {
            None
        };
        let column_meta = GeoParquetColumnMetadata {
            encoding: self.encoding,
            geometry_types: self.geometry_types.into_iter().collect(),
            crs: self.crs,
            bbox,
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

pub fn get_geometry_types(data_type: &GeoDataType) -> HashSet<GeoParquetGeometryType> {
    use GeoParquetGeometryType::*;
    let mut geometry_types = HashSet::new();

    match data_type {
        GeoDataType::Point(_, Dimension::XY) => {
            geometry_types.insert(Point);
        }
        GeoDataType::Point(_, Dimension::XYZ) => {
            geometry_types.insert(PointZ);
        }
        GeoDataType::LineString(_, Dimension::XY)
        | GeoDataType::LargeLineString(_, Dimension::XY) => {
            geometry_types.insert(LineString);
        }
        GeoDataType::LineString(_, Dimension::XYZ)
        | GeoDataType::LargeLineString(_, Dimension::XYZ) => {
            geometry_types.insert(LineStringZ);
        }
        GeoDataType::Polygon(_, Dimension::XY)
        | GeoDataType::LargePolygon(_, Dimension::XY)
        | GeoDataType::Rect(Dimension::XY) => {
            geometry_types.insert(Polygon);
        }
        GeoDataType::Polygon(_, Dimension::XYZ)
        | GeoDataType::LargePolygon(_, Dimension::XYZ)
        | GeoDataType::Rect(Dimension::XYZ) => {
            geometry_types.insert(PolygonZ);
        }
        GeoDataType::MultiPoint(_, Dimension::XY)
        | GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
            geometry_types.insert(MultiPoint);
        }
        GeoDataType::MultiPoint(_, Dimension::XYZ)
        | GeoDataType::LargeMultiPoint(_, Dimension::XYZ) => {
            geometry_types.insert(MultiPointZ);
        }
        GeoDataType::MultiLineString(_, Dimension::XY)
        | GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
            geometry_types.insert(MultiLineString);
        }
        GeoDataType::MultiLineString(_, Dimension::XYZ)
        | GeoDataType::LargeMultiLineString(_, Dimension::XYZ) => {
            geometry_types.insert(MultiLineStringZ);
        }
        GeoDataType::MultiPolygon(_, Dimension::XY)
        | GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
            geometry_types.insert(MultiPolygon);
        }
        GeoDataType::MultiPolygon(_, Dimension::XYZ)
        | GeoDataType::LargeMultiPolygon(_, Dimension::XYZ) => {
            geometry_types.insert(MultiPolygonZ);
        }
        GeoDataType::Mixed(_, _) | GeoDataType::LargeMixed(_, _) => {
            // We don't have access to the actual data here, so we can't inspect better than this.
        }
        GeoDataType::GeometryCollection(_, Dimension::XY)
        | GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
            geometry_types.insert(GeometryCollection);
        }
        GeoDataType::GeometryCollection(_, Dimension::XYZ)
        | GeoDataType::LargeGeometryCollection(_, Dimension::XYZ) => {
            geometry_types.insert(GeometryCollectionZ);
        }
        GeoDataType::WKB | GeoDataType::LargeWKB => {}
    };

    geometry_types
}

fn create_output_schema(
    input_schema: &Schema,
    columns: &HashMap<usize, ColumnInfo>,
) -> Arc<Schema> {
    let mut fields = input_schema.fields().to_vec();
    for (column_idx, column_info) in columns.iter() {
        let existing_field = input_schema.field(*column_idx);
        let output_field = create_output_field(
            column_info,
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

fn create_output_field(column_info: &ColumnInfo, name: String, nullable: bool) -> Field {
    use GeoParquetColumnEncoding as Encoding;
    use GeoParquetGeometryType::*;

    match column_info.encoding {
        Encoding::WKB => GeoDataType::WKB.to_field(name, nullable),
        Encoding::Point => {
            if column_info.geometry_types.contains(&PointZ) {
                GeoDataType::Point(CoordType::Separated, Dimension::XYZ).to_field(name, nullable)
            } else {
                GeoDataType::Point(CoordType::Separated, Dimension::XY).to_field(name, nullable)
            }
        }
        Encoding::LineString => {
            if column_info.geometry_types.contains(&LineStringZ) {
                GeoDataType::LineString(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                GeoDataType::LineString(CoordType::Separated, Dimension::XY)
                    .to_field(name, nullable)
            }
        }
        Encoding::Polygon => {
            if column_info.geometry_types.contains(&PolygonZ) {
                GeoDataType::Polygon(CoordType::Separated, Dimension::XYZ).to_field(name, nullable)
            } else {
                GeoDataType::Polygon(CoordType::Separated, Dimension::XY).to_field(name, nullable)
            }
        }
        Encoding::MultiPoint => {
            if column_info.geometry_types.contains(&MultiPointZ) {
                GeoDataType::MultiPoint(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                GeoDataType::MultiPoint(CoordType::Separated, Dimension::XY)
                    .to_field(name, nullable)
            }
        }
        Encoding::MultiLineString => {
            if column_info.geometry_types.contains(&MultiLineStringZ) {
                GeoDataType::MultiLineString(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                GeoDataType::MultiLineString(CoordType::Separated, Dimension::XY)
                    .to_field(name, nullable)
            }
        }
        Encoding::MultiPolygon => {
            if column_info.geometry_types.contains(&MultiPolygonZ) {
                GeoDataType::MultiPolygon(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                GeoDataType::MultiPolygon(CoordType::Separated, Dimension::XY)
                    .to_field(name, nullable)
            }
        }
    }
}
