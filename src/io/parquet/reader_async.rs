use crate::error::Result;
use crate::io::parquet::metadata::build_arrow_schema;
use crate::io::parquet::reader::GeoParquetReaderOptions;
use crate::table::GeoTable;

use futures::stream::TryStreamExt;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};

/// Asynchronously read a GeoParquet file to a GeoTable.
pub async fn read_geoparquet_async<R: AsyncFileReader + Unpin + Send + 'static>(
    reader: R,
    options: GeoParquetReaderOptions,
) -> Result<GeoTable> {
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_batch_size(options.batch_size);

    let (arrow_schema, geometry_column_index, target_geo_data_type) =
        build_arrow_schema(&builder, &options.coord_type)?;

    let stream = builder.build()?;
    let batches = stream.try_collect::<_>().await?;

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
    use tokio::fs::File;

    #[tokio::test]
    async fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet")
            .await
            .unwrap();
        let options = GeoParquetReaderOptions::new(65536, Default::default());
        let _output_geotable = read_geoparquet_async(file, options).await.unwrap();
    }
}
