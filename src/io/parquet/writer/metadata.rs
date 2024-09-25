use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{Field, Schema};
use serde_json::Value;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::array::metadata::{ArrayMetadata, Edges};
use crate::array::{AsNativeArray, CoordType, NativeArrayDyn};
use crate::datatypes::{Dimension, NativeType, SerializedType};
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
        data_type: &NativeType,
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

    /// Update the geometry types in the encoder for mixed arrays

    // TODO: for multi columns, should we do a check to see if there are non-multi geometries in
    // the file? E.g. check if the diff in geom_offsets is 1 for any row, in which case we should
    // write, e.g. Polygon in addition to MultiPolygon
    //
    // Note: for these multi columns, we should first check the geometry_types HashSet, because we
    // shouldn't compute that for every array if we see in the first that the data is both multi
    // and single polygons.

    pub fn update_geometry_types(&mut self, array: &ArrayRef, field: &Field) -> Result<()> {
        let array = NativeArrayDyn::from_arrow_array(array, field)?.into_inner();
        let array_ref = array.as_ref();

        // We only have to do this for mixed arrays because other arrays are statically known
        match array_ref.data_type() {
            NativeType::Mixed(_, Dimension::XY) => {
                let mixed_arr = array_ref.as_mixed::<2>();
                if mixed_arr.has_points() {
                    self.geometry_types.insert(GeoParquetGeometryType::Point);
                }
                if mixed_arr.has_line_strings() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::LineString);
                }
                if mixed_arr.has_polygons() {
                    self.geometry_types.insert(GeoParquetGeometryType::Polygon);
                }
                if mixed_arr.has_multi_points() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::MultiPoint);
                }
                if mixed_arr.has_multi_line_strings() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::MultiLineString);
                }
                if mixed_arr.has_multi_polygons() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::MultiPolygon);
                }
            }
            NativeType::Mixed(_, Dimension::XYZ) => {
                let mixed_arr = array_ref.as_mixed::<3>();
                if mixed_arr.has_points() {
                    self.geometry_types.insert(GeoParquetGeometryType::Point);
                }
                if mixed_arr.has_line_strings() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::LineString);
                }
                if mixed_arr.has_polygons() {
                    self.geometry_types.insert(GeoParquetGeometryType::Polygon);
                }
                if mixed_arr.has_multi_points() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::MultiPoint);
                }
                if mixed_arr.has_multi_line_strings() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::MultiLineString);
                }
                if mixed_arr.has_multi_polygons() {
                    self.geometry_types
                        .insert(GeoParquetGeometryType::MultiPolygon);
                }
            }
            _ => (),
        }
        Ok(())
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

pub fn get_geometry_types(data_type: &NativeType) -> HashSet<GeoParquetGeometryType> {
    use GeoParquetGeometryType::*;
    let mut geometry_types = HashSet::new();

    match data_type {
        NativeType::Point(_, Dimension::XY) => {
            geometry_types.insert(Point);
        }
        NativeType::Point(_, Dimension::XYZ) => {
            geometry_types.insert(PointZ);
        }
        NativeType::LineString(_, Dimension::XY)
        | NativeType::LargeLineString(_, Dimension::XY) => {
            geometry_types.insert(LineString);
        }
        NativeType::LineString(_, Dimension::XYZ)
        | NativeType::LargeLineString(_, Dimension::XYZ) => {
            geometry_types.insert(LineStringZ);
        }
        NativeType::Polygon(_, Dimension::XY)
        | NativeType::LargePolygon(_, Dimension::XY)
        | NativeType::Rect(Dimension::XY) => {
            geometry_types.insert(Polygon);
        }
        NativeType::Polygon(_, Dimension::XYZ)
        | NativeType::LargePolygon(_, Dimension::XYZ)
        | NativeType::Rect(Dimension::XYZ) => {
            geometry_types.insert(PolygonZ);
        }
        NativeType::MultiPoint(_, Dimension::XY)
        | NativeType::LargeMultiPoint(_, Dimension::XY) => {
            geometry_types.insert(MultiPoint);
        }
        NativeType::MultiPoint(_, Dimension::XYZ)
        | NativeType::LargeMultiPoint(_, Dimension::XYZ) => {
            geometry_types.insert(MultiPointZ);
        }
        NativeType::MultiLineString(_, Dimension::XY)
        | NativeType::LargeMultiLineString(_, Dimension::XY) => {
            geometry_types.insert(MultiLineString);
        }
        NativeType::MultiLineString(_, Dimension::XYZ)
        | NativeType::LargeMultiLineString(_, Dimension::XYZ) => {
            geometry_types.insert(MultiLineStringZ);
        }
        NativeType::MultiPolygon(_, Dimension::XY)
        | NativeType::LargeMultiPolygon(_, Dimension::XY) => {
            geometry_types.insert(MultiPolygon);
        }
        NativeType::MultiPolygon(_, Dimension::XYZ)
        | NativeType::LargeMultiPolygon(_, Dimension::XYZ) => {
            geometry_types.insert(MultiPolygonZ);
        }
        NativeType::Mixed(_, _) | NativeType::LargeMixed(_, _) => {
            // We don't have access to the actual data here, so we can't inspect better than this.
        }
        NativeType::GeometryCollection(_, Dimension::XY)
        | NativeType::LargeGeometryCollection(_, Dimension::XY) => {
            geometry_types.insert(GeometryCollection);
        }
        NativeType::GeometryCollection(_, Dimension::XYZ)
        | NativeType::LargeGeometryCollection(_, Dimension::XYZ) => {
            geometry_types.insert(GeometryCollectionZ);
        }
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
        Encoding::WKB => SerializedType::WKB.to_field(name, nullable),
        Encoding::Point => {
            if column_info.geometry_types.contains(&PointZ) {
                NativeType::Point(CoordType::Separated, Dimension::XYZ).to_field(name, nullable)
            } else {
                NativeType::Point(CoordType::Separated, Dimension::XY).to_field(name, nullable)
            }
        }
        Encoding::LineString => {
            if column_info.geometry_types.contains(&LineStringZ) {
                NativeType::LineString(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                NativeType::LineString(CoordType::Separated, Dimension::XY).to_field(name, nullable)
            }
        }
        Encoding::Polygon => {
            if column_info.geometry_types.contains(&PolygonZ) {
                NativeType::Polygon(CoordType::Separated, Dimension::XYZ).to_field(name, nullable)
            } else {
                NativeType::Polygon(CoordType::Separated, Dimension::XY).to_field(name, nullable)
            }
        }
        Encoding::MultiPoint => {
            if column_info.geometry_types.contains(&MultiPointZ) {
                NativeType::MultiPoint(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                NativeType::MultiPoint(CoordType::Separated, Dimension::XY).to_field(name, nullable)
            }
        }
        Encoding::MultiLineString => {
            if column_info.geometry_types.contains(&MultiLineStringZ) {
                NativeType::MultiLineString(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                NativeType::MultiLineString(CoordType::Separated, Dimension::XY)
                    .to_field(name, nullable)
            }
        }
        Encoding::MultiPolygon => {
            if column_info.geometry_types.contains(&MultiPolygonZ) {
                NativeType::MultiPolygon(CoordType::Separated, Dimension::XYZ)
                    .to_field(name, nullable)
            } else {
                NativeType::MultiPolygon(CoordType::Separated, Dimension::XY)
                    .to_field(name, nullable)
            }
        }
    }
}
