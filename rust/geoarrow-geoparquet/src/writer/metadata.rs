use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{Field, Schema, SchemaRef};
use geoarrow_array::GeoArrowType;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::crs::{CRSTransform, DefaultCRSTransform};
use geoarrow_array::error::Result;
use geoarrow_schema::{
    CoordType, Dimension, Edges, LineStringType, Metadata, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType, WkbType,
};
use serde_json::Value;

use crate::metadata::{
    GeoParquetColumnEncoding, GeoParquetColumnMetadata, GeoParquetGeometryType, GeoParquetMetadata,
};
use crate::total_bounds::BoundingRect;
use crate::writer::options::{GeoParquetWriterEncoding, GeoParquetWriterOptions};

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
    #[allow(clippy::borrowed_box)]
    pub fn try_new(
        name: String,
        writer_encoding: GeoParquetWriterEncoding,
        data_type: &GeoArrowType,
        array_meta: Metadata,
        crs_transform: Option<&Box<dyn CRSTransform>>,
    ) -> Result<Self> {
        let encoding = GeoParquetColumnEncoding::try_new(writer_encoding, data_type)?;
        let geometry_types = get_geometry_types(data_type);

        let crs = if let Some(crs_transform) = crs_transform {
            crs_transform.extract_projjson(array_meta.crs())?
        } else {
            DefaultCRSTransform::default().extract_projjson(array_meta.crs())?
        };
        let edges = array_meta.edges();

        Ok(Self {
            name,
            encoding,
            geometry_types,
            bbox: None,
            crs,
            edges,
        })
    }

    pub fn update_bbox(&mut self, new_bounds: &BoundingRect) {
        if let Some(existing_bounds) = self.bbox.as_mut() {
            existing_bounds.update(new_bounds)
        } else {
            self.bbox = Some(*new_bounds);
        }
    }

    /// Update the geometry types in the encoder for mixed arrays
    // TODO: for multi columns, should we do a check to see if there are non-multi geometries in
    // the file? E.g. check if the diff in geom_offsets is 1 for any row, in which case we should
    // write, e.g. Polygon in addition to MultiPolygon
    //
    // Note: for these multi columns, we should first check the geometry_types HashSet, because we
    // shouldn't compute that for every array if we see in the first that the data is both multi
    // and single polygons.
    pub fn update_geometry_types(&mut self, array: &ArrayRef, field: &Field) -> Result<()> {
        let array = from_arrow_array(array, field)?;
        let array_ref = array.as_ref();

        // We only have to do this for geometry arrays because other arrays are statically known
        if let GeoArrowType::Geometry(_) = array_ref.data_type() {
            // TODO: restore writing `geometry_types`.
            // The spec says "The geometry types of all geometries, or an empty array if they are
            // not known.". So it's valid for us to write an empty array, but in the future we
            // should restore writing known types.

            // let arr = array_ref.as_geometry();
            // if arr.has_points(Dimension::XY) || arr.has_points(Dimension::XYZ) {
            //     self.geometry_types.insert(GeoParquetGeometryType::Point);
            // }
            // if arr.has_line_strings(Dimension::XY) || arr.has_line_strings(Dimension::XYZ) {
            //     self.geometry_types
            //         .insert(GeoParquetGeometryType::LineString);
            // }
            // if arr.has_polygons(Dimension::XY) || arr.has_polygons(Dimension::XYZ) {
            //     self.geometry_types.insert(GeoParquetGeometryType::Polygon);
            // }
            // if arr.has_multi_points(Dimension::XY) || arr.has_multi_points(Dimension::XYZ) {
            //     self.geometry_types
            //         .insert(GeoParquetGeometryType::MultiPoint);
            // }
            // if arr.has_multi_line_strings(Dimension::XY)
            //     || arr.has_multi_line_strings(Dimension::XYZ)
            // {
            //     self.geometry_types
            //         .insert(GeoParquetGeometryType::MultiLineString);
            // }
            // if arr.has_multi_polygons(Dimension::XY) || arr.has_multi_polygons(Dimension::XYZ) {
            //     self.geometry_types
            //         .insert(GeoParquetGeometryType::MultiPolygon);
            // }
        }

        Ok(())
    }

    /// Returns (column_name, column_metadata)
    pub fn finish(self) -> (String, GeoParquetColumnMetadata) {
        let edges = self.edges.and_then(|edges| match edges {
            Edges::Spherical => Some("spherical".to_string()),
            _ => None,
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
    pub output_schema: SchemaRef,
    pub primary_column: Option<String>,
    pub columns: HashMap<usize, ColumnInfo>,
}

impl GeoParquetMetadataBuilder {
    pub fn try_new(schema: &Schema, options: &GeoParquetWriterOptions) -> Result<Self> {
        let mut columns = HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(ext_name) = field.metadata().get(EXTENSION_TYPE_NAME_KEY) {
                if !ext_name.starts_with("geoarrow") {
                    continue;
                }

                let column_name = schema.field(col_idx).name().clone();

                let array_meta =
                    if let Some(ext_meta) = field.metadata().get(EXTENSION_TYPE_METADATA_KEY) {
                        serde_json::from_str(ext_meta)?
                    } else {
                        Metadata::default()
                    };

                let geo_data_type = field.as_ref().try_into()?;

                let column_info = ColumnInfo::try_new(
                    column_name,
                    options.encoding,
                    &geo_data_type,
                    array_meta,
                    options.crs_transform.as_ref(),
                )?;

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

pub fn get_geometry_types(data_type: &GeoArrowType) -> HashSet<GeoParquetGeometryType> {
    use GeoParquetGeometryType::*;
    let mut geometry_types = HashSet::new();

    match data_type {
        GeoArrowType::Point(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(Point),
                Dimension::XYZ => geometry_types.insert(PointZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        GeoArrowType::LineString(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(LineString),
                Dimension::XYZ => geometry_types.insert(LineStringZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        GeoArrowType::Polygon(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(Polygon),
                Dimension::XYZ => geometry_types.insert(PolygonZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        // Also store rect as polygon
        GeoArrowType::Rect(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(Polygon),
                Dimension::XYZ => geometry_types.insert(PolygonZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        GeoArrowType::MultiPoint(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(MultiPoint),
                Dimension::XYZ => geometry_types.insert(MultiPointZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        GeoArrowType::MultiLineString(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(MultiLineString),
                Dimension::XYZ => geometry_types.insert(MultiLineStringZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        GeoArrowType::MultiPolygon(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(MultiPolygon),
                Dimension::XYZ => geometry_types.insert(MultiPolygonZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        GeoArrowType::Geometry(_) => {
            // We don't have access to the actual data here, so we can't inspect better than this.
        }
        GeoArrowType::GeometryCollection(t) => {
            match t.dimension() {
                Dimension::XY => geometry_types.insert(GeometryCollection),
                Dimension::XYZ => geometry_types.insert(GeometryCollectionZ),
                _ => todo!("Support more than XY and XYZ dimensions in Parquet writer"),
            };
        }
        _ => todo!("are serialized types supposed to be processed via this code path?"),
    };

    geometry_types
}

fn create_output_schema(input_schema: &Schema, columns: &HashMap<usize, ColumnInfo>) -> SchemaRef {
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
        Encoding::WKB => WkbType::new(Default::default()).to_field(name, nullable, false),
        Encoding::Point => {
            if column_info.geometry_types.contains(&PointZ) {
                PointType::new(CoordType::Separated, Dimension::XYZ, Default::default())
                    .to_field(name, nullable)
            } else {
                PointType::new(CoordType::Separated, Dimension::XY, Default::default())
                    .to_field(name, nullable)
            }
        }
        Encoding::LineString => {
            if column_info.geometry_types.contains(&LineStringZ) {
                LineStringType::new(CoordType::Separated, Dimension::XYZ, Default::default())
                    .to_field(name, nullable)
            } else {
                LineStringType::new(CoordType::Separated, Dimension::XY, Default::default())
                    .to_field(name, nullable)
            }
        }
        Encoding::Polygon => {
            if column_info.geometry_types.contains(&PolygonZ) {
                PolygonType::new(CoordType::Separated, Dimension::XYZ, Default::default())
                    .to_field(name, nullable)
            } else {
                PolygonType::new(CoordType::Separated, Dimension::XY, Default::default())
                    .to_field(name, nullable)
            }
        }
        Encoding::MultiPoint => {
            if column_info.geometry_types.contains(&MultiPointZ) {
                MultiPointType::new(CoordType::Separated, Dimension::XYZ, Default::default())
                    .to_field(name, nullable)
            } else {
                MultiPointType::new(CoordType::Separated, Dimension::XY, Default::default())
                    .to_field(name, nullable)
            }
        }
        Encoding::MultiLineString => {
            if column_info.geometry_types.contains(&MultiLineStringZ) {
                MultiLineStringType::new(CoordType::Separated, Dimension::XYZ, Default::default())
                    .to_field(name, nullable)
            } else {
                MultiLineStringType::new(CoordType::Separated, Dimension::XY, Default::default())
                    .to_field(name, nullable)
            }
        }
        Encoding::MultiPolygon => {
            if column_info.geometry_types.contains(&MultiPolygonZ) {
                MultiPolygonType::new(CoordType::Separated, Dimension::XYZ, Default::default())
                    .to_field(name, nullable)
            } else {
                MultiPolygonType::new(CoordType::Separated, Dimension::XY, Default::default())
                    .to_field(name, nullable)
            }
        }
    }
}
