use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;

#[derive(Clone)]
pub(crate) enum PyAsyncReader {
    // Python(PyObspecReader),
    ObjectStore(ParquetObjectReader),
}

impl AsyncFileReader for PyAsyncReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        match self {
            // Self::Python(reader) => reader.get_bytes(range),
            Self::ObjectStore(reader) => reader.get_bytes(range),
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        match self {
            // Self::Python(reader) => reader.get_byte_ranges(ranges),
            Self::ObjectStore(reader) => reader.get_byte_ranges(ranges),
        }
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        match self {
            // Self::Python(reader) => reader.get_metadata(),
            Self::ObjectStore(reader) => reader.get_metadata(),
        }
    }
}
