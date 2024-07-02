use std::sync::Arc;

use crate::error::Result;
use crate::io::parquet::reader::parse_table_geometries_to_native;
use crate::io::parquet::ParquetReaderOptions;
use crate::table::Table;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;

/// Read a GeoParquet file to a Table.
pub fn read_geoparquet<R: ChunkReader + 'static>(
    reader: R,
    options: ParquetReaderOptions,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let coord_type = options.coord_type;
    let builder = options.apply_to_builder(builder)?;
    let parquet_file_meta = builder.metadata().file_metadata().clone();
    let arrow_schema = builder.schema().clone();

    // TODO: actually infer output schema
    // let new_schema = Arc::new(Schema::empty());

    let reader = builder.build()?;
    let iter = reader.map(move |batch| {
        let mut table = Table::try_new(arrow_schema.clone(), vec![batch?])
            .map_err(|err| ArrowError::CastError(err.to_string()))?;
        parse_table_geometries_to_native(&mut table, &parquet_file_meta, &coord_type)
            .map_err(|err| ArrowError::CastError(err.to_string()))?;
        let (_schema, batches) = table.into_inner();
        assert_eq!(batches.len(), 1);
        Ok(batches.first().unwrap().clone())
    });

    Ok(Box::new(RecordBatchIterator::new(iter, new_schema)))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;

    #[test]
    #[cfg(feature = "parquet_compression")]
    fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
        let options = Default::default();
        let _output_ipc = read_geoparquet(file, options).unwrap();
    }
}
