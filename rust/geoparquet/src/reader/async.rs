use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_schema::SchemaRef;
use geoarrow_schema::error::GeoArrowResult;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};

use crate::reader::parse::{parse_record_batch, validate_target_schema};

/// A wrapper around a [`ParquetRecordBatchStream`] to apply GeoArrow metadata onto emitted
/// [`RecordBatch`]es.
///
/// Note that you have to call [`Self::read_stream`] to actually kick off the stream.
///
/// This will parse any geometries to their native representation.
pub struct GeoParquetRecordBatchStream<T: AsyncFileReader + Send + 'static> {
    stream: ParquetRecordBatchStream<T>,
    target_schema: SchemaRef,
}

impl<T: AsyncFileReader + Send + Unpin + 'static> GeoParquetRecordBatchStream<T> {
    /// Create a new [`GeoParquetRecordBatchStream`] from a [`ParquetRecordBatchStream`].
    ///
    /// This will validate that the target schema is compatible with the original schema.
    pub fn try_new(
        stream: ParquetRecordBatchStream<T>,
        target_schema: SchemaRef,
    ) -> GeoArrowResult<Self> {
        validate_target_schema(&stream.schema(), &target_schema)?;
        Ok(Self {
            stream,
            target_schema,
        })
    }
}

impl<T: AsyncFileReader + Send + Unpin + 'static> futures::Stream
    for GeoParquetRecordBatchStream<T>
{
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(
                parse_record_batch(batch, self.target_schema.clone()).map_err(|err| err.into()),
            )),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncFileReader + Unpin + Send + 'static> GeoParquetRecordBatchStream<T> {
    /// Returns the schema of this [`GeoParquetRecordBatchStream`].
    ///
    /// All [`RecordBatch`]es returned by this stream will have the same schema as returned from
    /// this method.
    pub fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }

    // /// Start a stream from the file.
    // ///
    // /// Each Arrow batch will be fetched and any geometry columns will be parsed into the GeoArrow
    // /// native representation.
    // pub fn read_stream(
    //     self,
    // ) -> impl Stream<Item = std::result::Result<RecordBatch, ArrowError>> + 'static {
    //     try_stream! {
    //         for await batch in self.stream {
    //             yield parse_record_batch(batch?, self.output_schema.clone()).map_err(|err| ArrowError::CastError(err.to_string()))?
    //         }
    //     }
    // }

    // /// Collect all batches into an in-memory table.
    // pub async fn read_table(self) -> GeoArrowResult<(Vec<RecordBatch>, SchemaRef)> {
    //     let output_schema = self.output_schema.clone();
    //     let batches = self.read_stream().try_collect::<_>().await?;
    //     Ok((batches, output_schema))
    // }
}
