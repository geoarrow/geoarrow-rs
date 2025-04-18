//! An asynchronous Parquet reader that is able to read and inspect remote files without
//! downloading them in entirety.

pub mod fetch;

use futures::channel::oneshot;
use futures::future::BoxFuture;
use object_store::coalesce_ranges;
use parquet::arrow::ProjectionMask;
use std::ops::Range;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::error::WasmResult;
use fetch::{range_from_end, range_from_start_and_length};

use arrow_wasm::{RecordBatch, Table};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::{FutureExt, stream};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStreamBuilder,
};

use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use reqwest::Client;

use async_trait::async_trait;

// This was used until we switched to object store for making requests. When we're happy with the
// object store implementation and believe it's stable, we can remove this.
#[allow(dead_code)]
#[async_trait(?Send)]
trait SharedIO<T: AsyncFileReader + Unpin + Clone + 'static> {
    fn generate_builder(
        reader: &T,
        meta: &ArrowReaderMetadata,
        batch_size: &usize,
        projection_mask: &Option<ProjectionMask>,
    ) -> ParquetRecordBatchStreamBuilder<T> {
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader.clone(), meta.clone())
                .with_batch_size(*batch_size)
                .with_projection(
                    projection_mask
                        .as_ref()
                        .unwrap_or(&ProjectionMask::all())
                        .clone(),
                );
        builder
    }

    async fn inner_read_row_group(
        &self,
        reader: &T,
        meta: &ArrowReaderMetadata,
        batch_size: &usize,
        projection_mask: &Option<ProjectionMask>,
        i: usize,
    ) -> WasmResult<Table> {
        let builder = Self::generate_builder(reader, meta, batch_size, projection_mask);
        let schema = builder.schema().clone();
        let stream = builder
            .with_row_groups(vec![i])
            .build()
            .map_err(|err| geoarrow_array::error::GeoArrowError::External(Box::new(err)))?;
        let results = stream.try_collect::<Vec<_>>().await.unwrap();

        // NOTE: This is not only one batch by default due to arrow-rs's default rechunking.
        // assert_eq!(results.len(), 1, "Expected one record batch");
        // Ok(RecordBatch::new(results.pop().unwrap()))
        Ok(Table::new(schema, results))
    }

    async fn inner_stream(
        &self,
        concurrency: Option<usize>,
        meta: &ArrowReaderMetadata,
        reader: &T,
        batch_size: &usize,
        projection_mask: &Option<ProjectionMask>,
    ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        use futures::StreamExt;
        let concurrency = concurrency.unwrap_or(1);
        let meta = meta.clone();
        let reader = reader.clone();
        let batch_size = *batch_size;
        let num_row_groups = meta.metadata().num_row_groups();
        let projection_mask = projection_mask.clone();
        let buffered_stream = stream::iter((0..num_row_groups).map(move |i| {
            let builder = Self::generate_builder(&reader, &meta, &batch_size, &projection_mask)
                .with_row_groups(vec![i]);
            builder.build().unwrap().try_collect::<Vec<_>>()
        }))
        .buffered(concurrency);
        let out_stream = buffered_stream.flat_map(|maybe_record_batches| {
            stream::iter(maybe_record_batches.unwrap())
                .map(|record_batch| Ok(RecordBatch::new(record_batch).into()))
        });
        Ok(wasm_streams::ReadableStream::from_stream(out_stream).into_raw())
    }
}

// #[wasm_bindgen]
// pub struct AsyncParquetFile {
//     reader: HTTPFileReader,
//     meta: ArrowReaderMetadata,
//     batch_size: usize,
//     projection_mask: Option<ProjectionMask>,
// }

// impl SharedIO<HTTPFileReader> for AsyncParquetFile {}

// #[wasm_bindgen]
// impl AsyncParquetFile {
//     #[wasm_bindgen(constructor)]
//     pub async fn new(url: String) -> WasmResult<AsyncParquetFile> {
//         let client = Client::new();
//         let mut reader = HTTPFileReader::new(url.clone(), client.clone(), 1024);
//         let meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
//         Ok(Self {
//             reader,
//             meta,
//             projection_mask: None,
//             batch_size: 1024,
//         })
//     }

//     #[wasm_bindgen(js_name = withBatchSize)]
//     pub fn with_batch_size(self, batch_size: usize) -> Self {
//         Self { batch_size, ..self }
//     }

//     #[wasm_bindgen(js_name = selectColumns)]
//     pub fn select_columns(self, columns: Vec<String>) -> WasmResult<AsyncParquetFile> {
//         let pq_schema = self.meta.parquet_schema();
//         let projection_mask = Some(generate_projection_mask(columns, pq_schema)?);
//         Ok(Self {
//             projection_mask,
//             ..self
//         })
//     }

//     // #[wasm_bindgen]
//     // pub fn metadata(&self) -> WasmResult<crate::metadata::ParquetMetaData> {
//     //     Ok(self.meta.metadata().as_ref().to_owned().into())
//     // }

//     #[wasm_bindgen(js_name = readRowGroup)]
//     pub async fn read_row_group(&self, i: usize) -> WasmResult<Table> {
//         self.inner_read_row_group(
//             &self.reader,
//             &self.meta,
//             &self.batch_size,
//             &self.projection_mask,
//             i,
//         )
//         .await
//     }

//     #[wasm_bindgen]
//     pub async fn stream(
//         &self,
//         concurrency: Option<usize>,
//     ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
//         self.inner_stream(
//             concurrency,
//             &self.meta,
//             &self.reader,
//             &self.batch_size,
//             &self.projection_mask,
//         )
//         .await
//     }
// }

#[derive(Debug, Clone)]
pub struct HTTPFileReader {
    url: String,
    client: Client,
    coalesce_byte_size: u64,
}

impl HTTPFileReader {
    pub fn new(url: String, client: Client, coalesce_byte_size: u64) -> Self {
        Self {
            url,
            client,
            coalesce_byte_size,
        }
    }
}

impl MetadataSuffixFetch for &mut HTTPFileReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let range_str = range_from_end(suffix);

            // Map reqwest error to parquet error
            // let map_err = |err| parquet::errors::ParquetError::External(Box::new(err));

            let bytes = make_range_request_with_client(
                self.url.to_string(),
                self.client.clone(),
                range_str,
            )
            .await
            .unwrap();

            Ok(bytes)
        }
        .boxed()
    }
}

async fn get_bytes_http(
    url: String,
    client: Client,
    range: Range<u64>,
) -> parquet::errors::Result<Bytes> {
    let range_str = range_from_start_and_length(range.start, range.end - range.start);

    // Map reqwest error to parquet error
    // let map_err = |err| parquet::errors::ParquetError::External(Box::new(err));

    let bytes = make_range_request_with_client(url, client, range_str)
        .await
        .unwrap();

    Ok(bytes)
}

impl AsyncFileReader for HTTPFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        get_bytes_http(self.url.clone(), self.client.clone(), range).boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            coalesce_ranges(
                &ranges,
                |range| get_bytes_http(self.url.clone(), self.client.clone(), range),
                self.coalesce_byte_size,
            )
            .await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_page_indexes(true)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

pub async fn make_range_request_with_client(
    url: String,
    client: Client,
    range_str: String,
) -> std::result::Result<Bytes, JsValue> {
    let (sender, receiver) = oneshot::channel();
    spawn_local(async move {
        let resp = client
            .get(url)
            .header("Range", range_str)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        let bytes = resp.bytes().await.unwrap();
        sender.send(bytes).unwrap();
    });
    let data = receiver.await.unwrap();
    Ok(data)
}
