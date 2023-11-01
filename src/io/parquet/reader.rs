use std::sync::Arc;

use crate::error::Result;
use crate::table::GeoTable;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::FileMetaData;
use parquet::file::reader::ChunkReader;

use crate::array::*;
use crate::io::parquet::geoparquet_metadata::GeoParquetMetadata;
use crate::GeometryArrayTrait;

enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl GeometryType {
    pub fn extension_field(&self) -> Arc<Field> {
        use GeometryType::*;
        match self {
            Point => PointArray::default().extension_field(),
            LineString => LineStringArray::<i32>::default().extension_field(),
            Polygon => PolygonArray::<i32>::default().extension_field(),
            MultiPoint => MultiPointArray::<i32>::default().extension_field(),
            MultiLineString => MultiLineStringArray::<i32>::default().extension_field(),
            MultiPolygon => MultiPolygonArray::<i32>::default().extension_field(),
        }
    }
}

impl From<&str> for GeometryType {
    fn from(value: &str) -> Self {
        match value {
            "Point" => GeometryType::Point,
            "LineString" => GeometryType::LineString,
            "Polygon" => GeometryType::Polygon,
            "MultiPoint" => GeometryType::MultiPoint,
            "MultiLineString" => GeometryType::MultiLineString,
            "MultiPolygon" => GeometryType::MultiPolygon,
            _ => panic!("Unsupported geometry type: {}", value),
        }
    }
}

fn parse_wkb_to_geoarrow(
    schema: &Schema,
    batch: RecordBatch,
    should_return_schema: bool,
    geometry_column_index: usize,
    geometry_type: &GeometryType,
) -> RecordBatch {
    let mut arrays = batch.columns();

    let wkb_array: WKBArray<i32> = arrays[geometry_column_index].as_ref().try_into().unwrap();
    let geom_array = match geometry_type {
        GeometryType::Point => GeometryArray::Point(wkb_array.try_into().unwrap()),
        GeometryType::LineString => GeometryArray::LineString(wkb_array.try_into().unwrap()),
        GeometryType::Polygon => GeometryArray::Polygon(wkb_array.try_into().unwrap()),
        GeometryType::MultiPoint => GeometryArray::MultiPoint(wkb_array.try_into().unwrap()),
        GeometryType::MultiLineString => {
            GeometryArray::MultiLineString(wkb_array.try_into().unwrap())
        }
        GeometryType::MultiPolygon => GeometryArray::MultiPolygon(wkb_array.try_into().unwrap()),
    };

    let extension_field = geom_array.extension_field();
    let geom_arr = geom_array.into_array_ref();
    arrays[geometry_column_index] = geom_arr;

    let returned_schema = if should_return_schema {
        let existing_field = &schema.fields[geometry_column_index];
        let new_field = Arc::new(
            extension_field
                .with_name(existing_field.name())
                .with_nullable(existing_field.is_nullable()),
        );
        let mut new_schema = schema.clone();
        new_schema.fields[geometry_column_index] = new_field;
        Some(new_schema)
    } else {
        None
    };

    // RecordBatch::try_new
    (returned_schema, Chunk::new(arrays))
}

fn parse_geoparquet_metadata(metadata: &FileMetaData) -> GeoParquetMetadata {
    let kv_metadata = metadata.key_value_metadata();

    if let Some(metadata) = kv_metadata {
        for kv in metadata {
            if kv.key == "geo" {
                if let Some(value) = &kv.value {
                    return serde_json::from_str(value).unwrap();
                }
            }
        }
    }

    panic!("expected a 'geo' key in GeoParquet metadata")
}

fn infer_geometry_type(meta: GeoParquetMetadata) -> GeometryType {
    let primary_column = meta.primary_column;
    let column_meta = meta.columns.get(&primary_column).unwrap();
    let geom_types = &column_meta.geometry_types;

    if geom_types.len() == 1 {
        return geom_types[0].as_str().into();
    }

    todo!()
}

/// Read a GeoParquet file to a GeoTable
pub fn read_geoparquet<R: ChunkReader>(mut reader: R) -> Result<GeoTable> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let parquet_reader = builder.build()?;

    let parquet_metadata = builder.metadata();
    let arrow_schema = builder.schema();

    // Parse GeoParquet metadata
    let geo_metadata = parse_geoparquet_metadata(parquet_metadata.file_metadata());
    let geometry_column_index = arrow_schema
        .fields
        .iter()
        .position(|field| field.name().as_ref() == geo_metadata.primary_column.as_ref())
        .unwrap();
    let inferred_geometry_type = infer_geometry_type(geo_metadata);

    let num_row_groups = parquet_metadata.num_row_groups();

    // Parse each row group from Parquet, one at a time, convert to GeoArrow, and store
    let (new_schema, new_chunks) = {
        let mut new_schema: Option<Schema> = None;
        let mut new_chunks = Vec::with_capacity(num_row_groups);
        for maybe_chunk in file_reader {
            let chunk = maybe_chunk?;
            if new_schema.is_none() {
                let (returned_schema, returned_chunk) = parse_wkb_to_geoarrow(
                    &schema,
                    chunk,
                    true,
                    geometry_column_index,
                    &inferred_geometry_type,
                );
                new_schema = returned_schema;
                new_chunks.push(returned_chunk)
            } else {
                let (_, returned_chunk) = parse_wkb_to_geoarrow(
                    &schema,
                    chunk,
                    false,
                    geometry_column_index,
                    &inferred_geometry_type,
                );
                new_chunks.push(returned_chunk)
            }
        }

        (new_schema.unwrap(), new_chunks)
    };

    GeoTable::try_new(new_schema, new_chunks, geometry_column_index)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;

    #[test]
    fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
        let _output_ipc = read_geoparquet(file).unwrap();
    }
}
