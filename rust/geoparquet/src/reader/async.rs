use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use futures::Stream;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};

use crate::reader::GeoParquetRecordBatchReader;
use crate::reader::parse::{parse_record_batch, validate_target_schema};

/// A wrapper around a [`ParquetRecordBatchStream`] to apply GeoArrow metadata onto emitted
/// [`RecordBatch`]es.
#[derive(Debug)]
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
        validate_target_schema(stream.schema(), &target_schema)?;
        Ok(Self {
            stream,
            target_schema,
        })
    }
}

impl<T: AsyncFileReader + Send + Unpin + 'static> Stream for GeoParquetRecordBatchStream<T> {
    type Item = Result<RecordBatch, ArrowError>;

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

    /// Fetches the next row group from the stream.
    ///
    /// Users can continue to call this function to get row groups and decode them concurrently.
    ///
    /// This is a wrapper around the upstream [`ParquetRecordBatchStream::next_row_group`].
    ///
    /// ## Notes
    ///
    /// `GeoParquetRecordBatchStream` should be used either as a `Stream` or with `next_row_group`;
    /// they should not be used simultaneously.
    ///
    /// ## Returns
    ///
    /// - `Ok(None)` if the stream has ended.
    /// - `Err(error)` if the stream has errored. All subsequent calls will return `Ok(None)`.
    /// - `Ok(Some(reader))` which holds all the data for the row group.
    pub async fn next_row_group(&mut self) -> GeoArrowResult<Option<GeoParquetRecordBatchReader>> {
        let maybe_reader = self
            .stream
            .next_row_group()
            .await
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;

        maybe_reader
            .map(|reader| GeoParquetRecordBatchReader::try_new(reader, self.target_schema.clone()))
            .transpose()
    }
}
