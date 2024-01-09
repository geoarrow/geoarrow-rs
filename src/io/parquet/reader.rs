use std::collections::HashSet;

use crate::array::CoordType;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::table::GeoTable;

use crate::io::parquet::geoparquet_metadata::GeoParquetMetadata;
use arrow_schema::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::FileMetaData;
use parquet::file::reader::ChunkReader;

// TODO: deduplicate with `resolve_types` in `downcast.rs`
fn infer_geo_data_type(
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
            "Point" => GeoDataType::Point(coord_type),
            "LineString" => GeoDataType::LineString(coord_type),
            "Polygon" => GeoDataType::Polygon(coord_type),
            "MultiPoint" => GeoDataType::MultiPoint(coord_type),
            "MultiLineString" => GeoDataType::MultiLineString(coord_type),
            "MultiPolygon" => GeoDataType::MultiPolygon(coord_type),
            "GeometryCollection" => GeoDataType::GeometryCollection(coord_type),
            _ => unreachable!(),
        })),
        2 => {
            if geometry_types.contains("Point") && geometry_types.contains("MultiPoint") {
                Ok(Some(GeoDataType::MultiPoint(coord_type)))
            } else if geometry_types.contains("LineString")
                && geometry_types.contains("MultiLineString")
            {
                Ok(Some(GeoDataType::MultiLineString(coord_type)))
            } else if geometry_types.contains("Polygon") && geometry_types.contains("MultiPolygon")
            {
                Ok(Some(GeoDataType::MultiPolygon(coord_type)))
            } else {
                Ok(Some(GeoDataType::Mixed(coord_type)))
            }
        }
        _ => Ok(Some(GeoDataType::Mixed(coord_type))),
    }
}

fn parse_geoparquet_metadata(
    metadata: &FileMetaData,
    schema: &Schema,
    coord_type: CoordType,
) -> Result<(usize, Option<GeoDataType>)> {
    let meta = GeoParquetMetadata::from_parquet_meta(metadata)?;
    let column_meta = meta
        .columns
        .get(&meta.primary_column)
        .ok_or(GeoArrowError::General(format!(
            "Expected {} in GeoParquet column metadata",
            &meta.primary_column
        )))?;

    let geometry_column_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == &meta.primary_column)
        .unwrap();
    let mut geometry_types = HashSet::with_capacity(column_meta.geometry_types.len());
    column_meta.geometry_types.iter().for_each(|t| {
        geometry_types.insert(t.as_str());
    });
    Ok((
        geometry_column_index,
        infer_geo_data_type(&geometry_types, coord_type)?,
    ))
}

pub struct GeoParquetReaderOptions {
    batch_size: usize,
    coord_type: CoordType,
}

impl GeoParquetReaderOptions {
    pub fn new(batch_size: usize, coord_type: CoordType) -> Self {
        Self {
            batch_size,
            coord_type,
        }
    }
}

pub fn read_geoparquet<R: ChunkReader + 'static>(
    reader: R,
    options: GeoParquetReaderOptions,
) -> Result<GeoTable> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(reader)?.with_batch_size(options.batch_size);

    let (arrow_schema, geometry_column_index, target_geo_data_type) = {
        let parquet_meta = builder.metadata();
        let arrow_schema = builder.schema().clone();
        let (geometry_column_index, target_geo_data_type) = parse_geoparquet_metadata(
            parquet_meta.file_metadata(),
            &arrow_schema,
            options.coord_type,
        )?;
        (arrow_schema, geometry_column_index, target_geo_data_type)
    };

    let reader = builder.build()?;

    let mut batches = vec![];
    for maybe_batch in reader {
        batches.push(maybe_batch?);
    }

    GeoTable::from_arrow(
        batches,
        arrow_schema,
        Some(geometry_column_index),
        target_geo_data_type,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;

    #[test]
    fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
        let options = GeoParquetReaderOptions::new(65536, Default::default());
        let _output_ipc = read_geoparquet(file, options).unwrap();
    }
}
