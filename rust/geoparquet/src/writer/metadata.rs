use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use geo_traits::GeometryTrait;
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_schema::crs::{CrsTransform, DefaultCrsTransform};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{CoordType, Dimension, Edges, GeoArrowType, Metadata, WkbType};
use serde_json::Value;

use crate::metadata::{
    GeoParquetBboxCovering, GeoParquetColumnEncoding, GeoParquetColumnMetadata, GeoParquetCovering,
    GeoParquetGeometryType, GeoParquetGeometryTypeAndDimension, GeoParquetMetadata,
};
use crate::total_bounds::BoundingRect;
use crate::writer::options::{GeoParquetWriterEncoding, GeoParquetWriterOptions};

// https://github.com/geoarrow/geoarrow-rs/pull/1159#issuecomment-2904610370
const INFERRED_PRIMARY_COLUMN_NAMES: [&str; 2] = ["geometry", "geography"];

/// Information for one geometry column being written to Parquet
pub(crate) struct ColumnInfo {
    /// The name of this geometry column
    pub(crate) name: String,

    /// The serialized encoding for this geometry column.
    pub(crate) encoding: GeoParquetColumnEncoding,

    /// The set of string geometry types for this geometry column
    pub(crate) geometry_types: HashSet<GeoParquetGeometryTypeAndDimension>,

    /// The bounding box of this column.
    pub(crate) bbox: Option<BoundingRect>,

    /// The PROJJSON CRS for this geometry column.
    pub(crate) crs: Option<Value>,

    /// If present, instructs consumers that edges follow a spherical path rather than a planar
    /// one. If this value is omitted, edges will be interpreted as planar.
    pub(crate) edges: Option<Edges>,

    /// If `None`, no covering is desired for this column. If `Some(s)`, then `s` is the top-level
    /// column name, stored as a struct, with child `xmin`, `ymin`, `xmax`, `ymax` columns.
    pub(crate) covering_name: Option<String>,

    /// This gets set in `create_output_schema`
    pub(crate) covering_field_idx: Option<usize>,
}

impl ColumnInfo {
    #[allow(clippy::borrowed_box)]
    pub(crate) fn try_new(
        name: String,
        writer_encoding: GeoParquetWriterEncoding,
        data_type: &GeoArrowType,
        metadata: &Metadata,
        crs_transform: Option<&Box<dyn CrsTransform>>,
        covering_name: Option<String>,
    ) -> GeoArrowResult<Self> {
        let encoding = GeoParquetColumnEncoding::try_new(writer_encoding, data_type)?;
        let geometry_types = get_geometry_types(data_type);

        let crs = if let Some(crs_transform) = crs_transform {
            crs_transform.extract_projjson(metadata.crs())?
        } else {
            DefaultCrsTransform::default().extract_projjson(metadata.crs())?
        };
        let edges = metadata.edges();

        Ok(Self {
            name,
            encoding,
            geometry_types,
            bbox: None,
            crs,
            edges,
            covering_name,
            covering_field_idx: None,
        })
    }

    pub(crate) fn update_bbox(&mut self, new_bounds: &BoundingRect) {
        if let Some(existing_bounds) = self.bbox.as_mut() {
            existing_bounds.update(new_bounds)
        } else {
            self.bbox = Some(*new_bounds);
        }
    }

    /// Update the geometry types in the encoder for arrays that do not have a statically-known
    /// type.
    // TODO: for multi columns, should we do a check to see if there are non-multi geometries in
    // the file? E.g. check if the diff in geom_offsets is 1 for any row, in which case we should
    // write, e.g. Polygon in addition to MultiPolygon. Perhaps we should in conjunction write a
    // MultiPoint with a single item as a WKB Point?
    //
    // Note: This has overlap with the non-public helper `get_type_ids` in `geoarrow-cast`. We may
    // want to stabilize some upstream APIs in the future to avoid this duplication.
    pub(crate) fn update_geometry_types(
        &mut self,
        array: &ArrayRef,
        field: &Field,
    ) -> GeoArrowResult<()> {
        let array = from_arrow_array(array, field)?;

        match array.data_type() {
            GeoArrowType::Geometry(_) => {
                let type_ids: HashSet<i8> =
                    HashSet::from_iter(array.as_geometry().type_ids().iter().copied());
                self.geometry_types.extend(
                    type_ids
                        .into_iter()
                        .map(GeoParquetGeometryTypeAndDimension::from_type_id),
                )
            }
            GeoArrowType::Wkb(_) => {
                let types = array
                    .as_wkb::<i32>()
                    .iter()
                    .flatten()
                    .map(|wkb| {
                        let wkb = wkb?;
                        let dim = wkb.dim().try_into()?;
                        let geom_type = GeoParquetGeometryType::from_geometry_trait(&wkb);
                        Ok(GeoParquetGeometryTypeAndDimension::new(geom_type, dim))
                    })
                    .collect::<GeoArrowResult<HashSet<GeoParquetGeometryTypeAndDimension>>>()?;
                self.geometry_types.extend(types);
            }
            GeoArrowType::LargeWkb(_) => {
                let types = array
                    .as_wkb::<i64>()
                    .iter()
                    .flatten()
                    .map(|wkb| {
                        let wkb = wkb?;
                        let dim = wkb.dim().try_into()?;
                        let geom_type = GeoParquetGeometryType::from_geometry_trait(&wkb);
                        Ok(GeoParquetGeometryTypeAndDimension::new(geom_type, dim))
                    })
                    .collect::<GeoArrowResult<HashSet<GeoParquetGeometryTypeAndDimension>>>()?;
                self.geometry_types.extend(types);
            }
            GeoArrowType::WkbView(_) => {
                let types = array
                    .as_wkb_view()
                    .iter()
                    .flatten()
                    .map(|wkb| {
                        let wkb = wkb?;
                        let dim = wkb.dim().try_into()?;
                        let geom_type = GeoParquetGeometryType::from_geometry_trait(&wkb);
                        Ok(GeoParquetGeometryTypeAndDimension::new(geom_type, dim))
                    })
                    .collect::<GeoArrowResult<HashSet<GeoParquetGeometryTypeAndDimension>>>()?;
                self.geometry_types.extend(types);
            }
            GeoArrowType::Wkt(_) => {
                let types = array
                    .as_wkt::<i32>()
                    .inner()
                    .iter()
                    .flatten()
                    .map(|s| {
                        let (wkt_type, wkt_dim) =
                            wkt::infer_type(s).map_err(ArrowError::CastError)?;
                        let geom_type = GeoParquetGeometryTypeAndDimension::new(
                            wkt_type_to_geoparquet_type(wkt_type),
                            wkt_dim_to_geoarrow_dim(wkt_dim),
                        );
                        Ok(geom_type)
                    })
                    .collect::<GeoArrowResult<HashSet<GeoParquetGeometryTypeAndDimension>>>()?;
                self.geometry_types.extend(types);
            }
            GeoArrowType::LargeWkt(_) => {
                let types = array
                    .as_wkt::<i64>()
                    .inner()
                    .iter()
                    .flatten()
                    .map(|s| {
                        let (wkt_type, wkt_dim) =
                            wkt::infer_type(s).map_err(ArrowError::CastError)?;
                        let geom_type = GeoParquetGeometryTypeAndDimension::new(
                            wkt_type_to_geoparquet_type(wkt_type),
                            wkt_dim_to_geoarrow_dim(wkt_dim),
                        );
                        Ok(geom_type)
                    })
                    .collect::<GeoArrowResult<HashSet<GeoParquetGeometryTypeAndDimension>>>()?;
                self.geometry_types.extend(types);
            }
            GeoArrowType::WktView(_) => {
                let types = array
                    .as_wkt_view()
                    .inner()
                    .iter()
                    .flatten()
                    .map(|s| {
                        let (wkt_type, wkt_dim) =
                            wkt::infer_type(s).map_err(ArrowError::CastError)?;
                        let geom_type = GeoParquetGeometryTypeAndDimension::new(
                            wkt_type_to_geoparquet_type(wkt_type),
                            wkt_dim_to_geoarrow_dim(wkt_dim),
                        );
                        Ok(geom_type)
                    })
                    .collect::<GeoArrowResult<HashSet<GeoParquetGeometryTypeAndDimension>>>()?;
                self.geometry_types.extend(types);
            }
            _ => {}
        };

        Ok(())
    }

    /// Returns (column_name, column_metadata)
    pub(crate) fn finish(self) -> (String, GeoParquetColumnMetadata) {
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
        let covering = if let Some(covering_name) = self.covering_name {
            let bbox_covering = GeoParquetBboxCovering {
                xmin: vec![covering_name.clone(), "xmin".to_string()],
                ymin: vec![covering_name.clone(), "ymin".to_string()],
                zmin: None,
                xmax: vec![covering_name.clone(), "xmax".to_string()],
                ymax: vec![covering_name.clone(), "ymax".to_string()],
                zmax: None,
            };
            Some(GeoParquetCovering {
                bbox: bbox_covering,
            })
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
            covering,
        };
        (self.name, column_meta)
    }
}

fn wkt_type_to_geoparquet_type(wkt_type: wkt::types::GeometryType) -> GeoParquetGeometryType {
    match wkt_type {
        wkt::types::GeometryType::Point => GeoParquetGeometryType::Point,
        wkt::types::GeometryType::LineString => GeoParquetGeometryType::LineString,
        wkt::types::GeometryType::Polygon => GeoParquetGeometryType::Polygon,
        wkt::types::GeometryType::MultiPoint => GeoParquetGeometryType::MultiPoint,
        wkt::types::GeometryType::MultiLineString => GeoParquetGeometryType::MultiLineString,
        wkt::types::GeometryType::MultiPolygon => GeoParquetGeometryType::MultiPolygon,
        wkt::types::GeometryType::GeometryCollection => GeoParquetGeometryType::GeometryCollection,
    }
}

fn wkt_dim_to_geoarrow_dim(wkt_dim: wkt::types::Dimension) -> Dimension {
    match wkt_dim {
        wkt::types::Dimension::XY => Dimension::XY,
        wkt::types::Dimension::XYZ => Dimension::XYZ,
        wkt::types::Dimension::XYM => Dimension::XYM,
        wkt::types::Dimension::XYZM => Dimension::XYZM,
    }
}

pub(crate) struct GeoParquetMetadataBuilder {
    pub(crate) output_schema: SchemaRef,
    pub(crate) primary_column: String,
    pub(crate) columns: HashMap<usize, ColumnInfo>,
}

impl GeoParquetMetadataBuilder {
    pub(crate) fn try_new(
        schema: &Schema,
        options: &GeoParquetWriterOptions,
    ) -> GeoArrowResult<Self> {
        let mut columns = HashMap::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(ext_name) = field.metadata().get(EXTENSION_TYPE_NAME_KEY) {
                if !ext_name.starts_with("geoarrow") {
                    continue;
                }

                let column_name = schema.field(col_idx).name().clone();
                let geo_data_type = field.as_ref().try_into()?;

                let column_encoding = options
                    .column_properties
                    .get(&column_name)
                    .map_or(options.default_column_properties.encoding, |props| {
                        props.encoding
                    })
                    .unwrap_or_default();

                let generate_covering = options
                    .column_properties
                    .get(&column_name)
                    .map_or(
                        options.default_column_properties.generate_covering,
                        |props| props.generate_covering,
                    )
                    .unwrap_or(false);

                let covering_name = if generate_covering {
                    let covering_name = options
                        .column_properties
                        .get(&column_name)
                        .and_then(|props| props.covering_name.as_ref())
                        .or(options.default_column_properties.covering_name.as_ref())
                        .cloned();
                    if let Some(covering_name) = covering_name {
                        Some(covering_name)
                    } else if INFERRED_PRIMARY_COLUMN_NAMES.contains(&column_name.as_str()) {
                        Some("bbox".to_string())
                    } else {
                        Some(format!("{column_name}_bbox"))
                    }
                } else {
                    None
                };

                let column_info = ColumnInfo::try_new(
                    column_name,
                    column_encoding,
                    &geo_data_type,
                    geo_data_type.metadata(),
                    options.crs_transform.as_ref(),
                    covering_name,
                )?;

                columns.insert(col_idx, column_info);
            }
        }

        if columns.is_empty() {
            return Err(GeoArrowError::GeoParquet(
                "No geometry columns found in schema".to_string(),
            ));
        }

        let primary_column = if let Some(primary_column) = options.primary_column.as_deref() {
            if !columns
                .values()
                .any(|column_info| column_info.name == primary_column)
            {
                return Err(GeoArrowError::GeoParquet(format!(
                    "Designated primary column: {primary_column} does not exist as a geometry field in schema",
                )));
            }

            primary_column.to_string()
        } else if let Some(column_info) = columns
            .values()
            .find(|column_info| INFERRED_PRIMARY_COLUMN_NAMES.contains(&column_info.name.as_str()))
        {
            column_info.name.clone()
        } else {
            // Make it deterministic which key we use.
            let mut geom_column_names: Vec<_> =
                columns.values().map(|col| col.name.as_str()).collect();

            // We already checked for empty columns
            assert!(!geom_column_names.is_empty());
            geom_column_names.sort();
            geom_column_names
                .first()
                .expect("No geometry columns when finishing GeoParquetMetadataBuilder")
                .to_string()
        };

        let output_schema = create_output_schema(schema, &mut columns);
        Ok(Self {
            primary_column,
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

    /// Consume this builder, converting into [GeoParquetMetadata].
    pub(crate) fn finish(self) -> GeoParquetMetadata {
        let mut columns = HashMap::with_capacity(self.columns.len());
        for column_info in self.columns.into_values() {
            let (column_name, column_meta) = column_info.finish();
            columns.insert(column_name, column_meta);
        }

        GeoParquetMetadata {
            version: "1.1.0".to_string(),
            primary_column: self.primary_column,
            columns,
        }
    }
}

pub(crate) fn get_geometry_types(
    data_type: &GeoArrowType,
) -> HashSet<GeoParquetGeometryTypeAndDimension> {
    use GeoParquetGeometryType::*;
    let mut geometry_types = HashSet::new();

    match data_type {
        GeoArrowType::Point(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(Point, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::LineString(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(LineString, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::Polygon(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(Polygon, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        // Also store rect as polygon
        GeoArrowType::Rect(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(Polygon, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::MultiPoint(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(MultiPoint, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::MultiLineString(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(MultiLineString, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::MultiPolygon(t) => {
            let gpq_typ = GeoParquetGeometryTypeAndDimension::new(MultiPolygon, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::GeometryCollection(t) => {
            let gpq_typ =
                GeoParquetGeometryTypeAndDimension::new(GeometryCollection, t.dimension());
            geometry_types.insert(gpq_typ);
        }
        GeoArrowType::Geometry(_)
        | GeoArrowType::Wkb(_)
        | GeoArrowType::LargeWkb(_)
        | GeoArrowType::WkbView(_)
        | GeoArrowType::Wkt(_)
        | GeoArrowType::LargeWkt(_)
        | GeoArrowType::WktView(_) => {
            // We don't have access to the actual data here, so we can't inspect better than this.
        }
    };

    geometry_types
}

fn create_output_schema(
    input_schema: &Schema,
    columns: &mut HashMap<usize, ColumnInfo>,
) -> SchemaRef {
    let mut fields = input_schema.fields().to_vec();
    for (column_idx, column_info) in columns.iter_mut() {
        let existing_field = input_schema.field(*column_idx);
        let output_field = create_output_field(
            column_info,
            existing_field.name().clone(),
            // For now we always create nullable geometry fields
            true,
        );
        fields[*column_idx] = output_field.into();

        if let Some(covering_name) = column_info.covering_name.as_deref() {
            let covering_field = create_covering_field(covering_name);
            column_info.covering_field_idx = Some(fields.len());
            fields.push(covering_field.into());
        }
    }

    Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

fn create_output_field(column_info: &ColumnInfo, name: String, nullable: bool) -> Field {
    use GeoParquetColumnEncoding as Encoding;

    match column_info.encoding {
        Encoding::WKB => Field::new(name, DataType::Binary, nullable)
            .with_extension_type(WkbType::new(Default::default())),
        // A native encoding
        _ => {
            assert_eq!(column_info.geometry_types.len(), 1);
            let gpq_type = column_info.geometry_types.iter().next().unwrap();
            let ga_type = gpq_type.to_data_type(CoordType::Separated, Default::default());
            ga_type.to_field(name, nullable)
        }
    }
}

fn create_covering_field(covering_name: &str) -> Field {
    let struct_fields = vec![
        Field::new("xmin", DataType::Float64, false),
        Field::new("ymin", DataType::Float64, false),
        Field::new("xmax", DataType::Float64, false),
        Field::new("ymax", DataType::Float64, false),
    ];
    Field::new(covering_name, DataType::Struct(struct_fields.into()), true)
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;
    use geoarrow_schema::error::GeoArrowError;
    use geoarrow_schema::{Dimension, PointType};

    use super::GeoParquetMetadataBuilder;
    use crate::writer::options::GeoParquetWriterOptions;

    #[test]
    fn primary_column_not_geometry() {
        let schema = Schema::empty();
        let options = GeoParquetWriterOptions {
            primary_column: Some("not-a-geometry-column".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            GeoParquetMetadataBuilder::try_new(&schema, &options),
            Err(GeoArrowError::GeoParquet(_))
        ));
    }

    #[test]
    fn primary_column_none_no_geometry() {
        let schema = Schema::empty();
        let options = GeoParquetWriterOptions::default();
        assert!(GeoParquetMetadataBuilder::try_new(&schema, &options).is_err());
    }

    #[test]
    fn primary_column_none() {
        let field = PointType::new(Dimension::XY, Default::default()).to_field("anything", false);
        let schema = Schema::new(vec![field]);
        let options = GeoParquetWriterOptions::default();
        let metadata = GeoParquetMetadataBuilder::try_new(&schema, &options)
            .unwrap()
            .finish();
        assert_eq!(metadata.primary_column, "anything");
    }

    #[test]
    fn primary_column_none_default_to_geometry() {
        let field_a = PointType::new(Dimension::XY, Default::default()).to_field("anything", false);
        let field_b = PointType::new(Dimension::XY, Default::default()).to_field("geometry", false);
        let schema = Schema::new(vec![field_a, field_b]);
        let options = GeoParquetWriterOptions::default();
        let metadata = GeoParquetMetadataBuilder::try_new(&schema, &options)
            .unwrap()
            .finish();
        assert_eq!(metadata.primary_column, "geometry");
    }

    #[test]
    fn primary_column() {
        let field_a = PointType::new(Dimension::XY, Default::default()).to_field("anything", false);
        let field_b = PointType::new(Dimension::XY, Default::default()).to_field("geometry", false);
        let schema = Schema::new(vec![field_a, field_b]);
        let options = GeoParquetWriterOptions {
            primary_column: Some("anything".to_string()),
            ..Default::default()
        };
        let metadata = GeoParquetMetadataBuilder::try_new(&schema, &options)
            .unwrap()
            .finish();
        assert_eq!(metadata.primary_column, "anything");
    }
}
