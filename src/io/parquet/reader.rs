use crate::error::Result;
use crate::table::GeoTable;
use arrow2::io::parquet::read::{
    infer_schema, read_metadata as parquet_read_metadata, FileReader as ParquetFileReader,
};
use std::io::{Read, Seek};

use crate::array::*;
use crate::io::parquet::geoparquet_metadata::GeoParquetMetadata;
use crate::GeometryArrayTrait;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Schema};
use arrow2::io::parquet::write::FileMetaData;

enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl GeometryType {
    pub fn _data_type(&self) -> DataType {
        use GeometryType::*;
        match self {
            Point => PointArray::default().extension_type(),
            LineString => LineStringArray::<i32>::default().extension_type(),
            Polygon => PolygonArray::<i32>::default().extension_type(),
            MultiPoint => MultiPointArray::<i32>::default().extension_type(),
            MultiLineString => MultiLineStringArray::<i32>::default().extension_type(),
            MultiPolygon => MultiPolygonArray::<i32>::default().extension_type(),
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
    chunk: Chunk<Box<dyn Array>>,
    should_return_schema: bool,
    geometry_column_index: usize,
    geometry_type: &GeometryType,
) -> (Option<Schema>, Chunk<Box<dyn Array>>) {
    let mut arrays = chunk.into_arrays();

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

    let extension_type = geom_array.extension_type();
    let geom_arr = geom_array.into_array_ref();
    arrays[geometry_column_index] = geom_arr;

    let returned_schema = if should_return_schema {
        let existing_field = &schema.fields[geometry_column_index];
        let mut new_field = existing_field.clone();
        new_field.data_type = extension_type;
        let mut new_schema = schema.clone();
        new_schema.fields[geometry_column_index] = new_field;
        Some(new_schema)
    } else {
        None
    };

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

pub fn read_geoparquet<R: Read + Seek>(mut reader: R) -> Result<GeoTable> {
    // Create Parquet reader
    let metadata = parquet_read_metadata(&mut reader)?;

    // Infer Arrow Schema
    let schema = infer_schema(&metadata)?;

    // Parse GeoParquet metadata
    let geo_metadata = parse_geoparquet_metadata(&metadata);
    let geometry_column_index = schema
        .fields
        .iter()
        .position(|field| field.name == geo_metadata.primary_column)
        .unwrap();
    let inferred_geometry_type = infer_geometry_type(geo_metadata);

    let num_row_groups = metadata.row_groups.len();
    let file_reader = ParquetFileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

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
    use std::io::BufReader;

    #[test]
    fn nybb() {
        let file = BufReader::new(File::open("fixtures/geoparquet/nybb.parquet").unwrap());
        let _output_ipc = read_geoparquet(file).unwrap();
    }
}
