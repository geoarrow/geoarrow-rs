use crate::error::Result;
use crate::io::parquet::geoparquet_metadata::build_arrow_schema;
use crate::io::parquet::reader::GeoParquetReaderOptions;
use crate::table::GeoTable;

use futures::stream::TryStreamExt;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};

pub async fn read_geoparquet_async<T: AsyncFileReader + Unpin + Send + 'static>(
    input: T,
    options: GeoParquetReaderOptions,
) -> Result<GeoTable> {
    let builder = ParquetRecordBatchStreamBuilder::new(input)
        .await?
        .with_batch_size(options.batch_size);

    let (arrow_schema, geometry_column_index, target_geo_data_type) =
        build_arrow_schema(&builder, &options.coord_type);

    let stream = builder.build()?;
    let batches = stream.try_collect::<_>().await?;

    GeoTable::from_arrow(
        batches,
        arrow_schema,
        Some(geometry_column_index),
        target_geo_data_type,
    )
}
