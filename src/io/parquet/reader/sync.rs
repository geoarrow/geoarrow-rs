use crate::error::Result;
use crate::io::parquet::metadata::build_arrow_schema;
use crate::io::parquet::GeoParquetReaderOptions;
use crate::table::GeoTable;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;

/// Read a GeoParquet file to a GeoTable.
pub fn read_geoparquet<R: ChunkReader + 'static>(
    reader: R,
    options: GeoParquetReaderOptions,
) -> Result<GeoTable> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(reader)?.with_batch_size(options.batch_size);

    let (arrow_schema, geometry_column_index, target_geo_data_type) =
        build_arrow_schema(&builder, &options.coord_type)?;

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
        let options = Default::default();
        let _output_ipc = read_geoparquet(file, options).unwrap();
    }
}
