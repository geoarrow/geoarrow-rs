use crate::error::Result;
use crate::io::parquet::metadata::build_arrow_schema;
use crate::io::parquet::ParquetReaderOptions;
use crate::table::Table;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;

/// Read a GeoParquet file to a Table.
pub fn read_geoparquet<R: ChunkReader + 'static>(
    reader: R,
    options: ParquetReaderOptions,
) -> Result<Table> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let coord_type = options.coord_type;
    let builder = options.apply_to_builder(builder)?;

    let (arrow_schema, geometry_column_index, target_geo_data_type) =
        build_arrow_schema(&builder, &coord_type)?;

    let reader = builder.build()?;

    let mut batches = vec![];
    for maybe_batch in reader {
        batches.push(maybe_batch?);
    }

    let mut table = Table::try_new(arrow_schema, batches)?;
    table.parse_geometry_to_native(geometry_column_index, target_geo_data_type)?;
    Ok(table)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;

    #[test]
    fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
        let options = Default::default();
        let _output_ipc = read_geoparquet(file, options).unwrap();
    }
}
