use crate::data::RectData;
use crate::error::{GeoArrowWasmError, WasmResult};
use crate::io::parquet::options::JsParquetReaderOptions;
use arrow_wasm::{RecordBatch, Table};
use futures::stream::StreamExt;
use geoarrow::geo_traits::{CoordTrait, RectTrait};
use geoarrow::io::parquet::metadata::GeoParquetBboxCovering;
use geoarrow::io::parquet::{
    GeoParquetDatasetMetadata, GeoParquetReaderMetadata, GeoParquetReaderOptions,
    GeoParquetRecordBatchStream, GeoParquetRecordBatchStreamBuilder,
};
use object_store::{ObjectMeta, ObjectStore};
use object_store_wasm::http::HttpStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::ParquetObjectReader;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ParquetFile {
    object_meta: object_store::ObjectMeta,
    geoparquet_meta: GeoParquetReaderMetadata,
    store: Arc<dyn ObjectStore>,
}

#[wasm_bindgen]
impl ParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> WasmResult<ParquetFile> {
        let parsed_url = Url::parse(&url)?;
        let base_url = Url::parse(&parsed_url.origin().unicode_serialization())?;
        let store = Arc::new(HttpStore::new(base_url));
        let location = object_store::path::Path::parse(parsed_url.path())?;
        let object_meta = store.head(&location).await?;
        let mut reader = ParquetObjectReader::new(store.clone(), object_meta.clone());
        let arrow_meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        let geoparquet_meta = GeoParquetReaderMetadata::new(arrow_meta);
        Ok(Self {
            object_meta,
            geoparquet_meta,
            store,
        })
    }

    /// The number of rows in this file.
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.geoparquet_meta.num_rows()
    }

    /// The number of row groups in this file.
    #[wasm_bindgen(getter, js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.geoparquet_meta.num_row_groups()
    }

    /// Get the bounds of a single row group.
    ///
    /// This fetches bounds for the row group from the column statistics in the row group metadata.
    #[wasm_bindgen(js_name = rowGroupBounds)]
    pub fn row_group_bounds(
        &self,
        row_group_idx: usize,
        bbox_paths: JsValue,
    ) -> WasmResult<Option<Vec<f64>>> {
        let paths: Option<GeoParquetBboxCovering> = serde_wasm_bindgen::from_value(bbox_paths)?;
        if let Some(bounds) = self
            .geoparquet_meta
            .row_group_bounds(row_group_idx, paths.as_ref())?
        {
            Ok(Some(vec![
                bounds.lower().x(),
                bounds.lower().y(),
                bounds.upper().x(),
                bounds.upper().y(),
            ]))
        } else {
            Ok(None)
        }
    }

    /// Get the bounds of all row groups.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    #[wasm_bindgen(js_name = rowGroupsBounds)]
    pub fn row_groups_bounds(&self, bbox_paths: JsValue) -> WasmResult<RectData> {
        let paths: Option<GeoParquetBboxCovering> = serde_wasm_bindgen::from_value(bbox_paths)?;
        let bounds = self.geoparquet_meta.row_groups_bounds(paths.as_ref())?;
        Ok(bounds.into())
    }

    /// Access the bounding box of the given column for the entire file
    ///
    /// If no column name is passed, retrieves the bbox from the primary geometry column.
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    #[wasm_bindgen(js_name = fileBbox)]
    pub fn file_bbox(&self, column_name: Option<String>) -> WasmResult<Option<Vec<f64>>> {
        let name = column_name.as_deref();
        let bbox = self.geoparquet_meta.file_bbox(name)?;
        Ok(bbox.map(|b| b.to_vec()))
    }

    #[wasm_bindgen]
    pub async fn read(&self, options: JsValue) -> WasmResult<Table> {
        let reader = ParquetObjectReader::new(self.store.clone(), self.object_meta.clone());
        let options: Option<JsParquetReaderOptions> = serde_wasm_bindgen::from_value(options)?;
        let stream = GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
            reader,
            self.geoparquet_meta.clone(),
            options.unwrap_or_default().into(),
        )
        .build()?;
        let table = stream.read_table().await?;
        let (batches, schema) = table.into_inner();
        Ok(Table::new(schema, batches))
    }
    #[wasm_bindgen]
    pub async fn read_stream(
        &self,
        options: JsValue,
    ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
        let reader = ParquetObjectReader::new(self.store.clone(), self.object_meta.clone());
        let options: Option<JsParquetReaderOptions> = serde_wasm_bindgen::from_value(options)?;
        let stream = GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
            reader,
            self.geoparquet_meta.clone(),
            options.unwrap_or_default().into(),
        )
        .build()?;

        let out_stream = stream
            .read_stream()
            .map(|maybe_batch| {
                let batch = maybe_batch.map_err(JsError::from)?;
                Ok(RecordBatch::new(batch).into())
            })
            .boxed_local();
        Ok(wasm_streams::ReadableStream::from_stream(out_stream).into_raw())
    }
}

#[wasm_bindgen]
pub struct ParquetDataset {
    meta: GeoParquetDatasetMetadata,
    store: Arc<dyn ObjectStore>,
}

/// Create a reader per path with the given ObjectStore instance.
async fn fetch_arrow_metadata_objects(
    paths: Vec<String>,
    store: Arc<dyn ObjectStore>,
) -> Result<HashMap<String, ArrowReaderMetadata>, GeoArrowWasmError> {
    let paths: Vec<object_store::path::Path> = paths.into_iter().map(|path| path.into()).collect();
    let object_meta_futures = paths.iter().map(|path| store.head(path));
    let object_metas = futures::future::join_all(object_meta_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, object_store::Error>>()?;
    let mut readers = object_metas
        .into_iter()
        .map(|meta| ParquetObjectReader::new(store.clone(), meta))
        .collect::<Vec<_>>();
    let parquet_meta_futures = readers
        .iter_mut()
        .map(|reader| ArrowReaderMetadata::load_async(reader, Default::default()));
    let parquet_metas = futures::future::join_all(parquet_meta_futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, parquet::errors::ParquetError>>()?;

    let mut hashmap: HashMap<String, ArrowReaderMetadata> = HashMap::new();
    for (path, arrow_meta) in paths.iter().zip(parquet_metas) {
        hashmap.insert(path.to_string(), arrow_meta);
    }

    Ok(hashmap)
}

impl ParquetDataset {
    fn to_readers(
        &self,
        geo_options: GeoParquetReaderOptions,
    ) -> Result<Vec<GeoParquetRecordBatchStream<ParquetObjectReader>>, geoarrow::error::GeoArrowError>
    {
        self.meta
            .to_stream_builders(
                |path| {
                    let object_meta = ObjectMeta {
                        location: path.into(),
                        last_modified: Default::default(),
                        // NOTE: Usually we'd need to know the file size of each object, but since we
                        // already have the Parquet metadata, this should be ok
                        size: 0,
                        e_tag: None,
                        version: None,
                    };
                    ParquetObjectReader::new(self.store.clone(), object_meta)
                },
                geo_options,
            )
            .into_iter()
            .map(|builder| builder.build())
            .collect()
    }
}

#[wasm_bindgen]
impl ParquetDataset {
    #[wasm_bindgen(constructor)]
    pub async fn new(root_url: String, fragment_urls: Vec<String>) -> WasmResult<ParquetDataset> {
        let store = Arc::new(HttpStore::new(Url::parse(&root_url)?));
        let meta = fetch_arrow_metadata_objects(fragment_urls, store.clone()).await?;

        Ok(Self {
            meta: GeoParquetDatasetMetadata::from_files(meta)?,
            store,
        })
    }

    /// The total number of rows across all files.
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.meta.num_rows()
    }

    /// The total number of row groups across all files
    #[wasm_bindgen(getter, js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.meta.num_row_groups()
    }

    #[wasm_bindgen]
    pub async fn read(&self, options: JsValue) -> WasmResult<Table> {
        let options: Option<JsParquetReaderOptions> = serde_wasm_bindgen::from_value(options)?;
        let readers = self.to_readers(options.unwrap_or_default().into())?;
        let output_schema = self.meta.resolved_schema(Default::default())?;

        let request_futures = readers.into_iter().map(|reader| reader.read_table());
        let tables = futures::future::join_all(request_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, geoarrow::error::GeoArrowError>>()?;

        let mut all_batches = vec![];
        tables.into_iter().for_each(|table| {
            let (table_batches, _schema) = table.into_inner();
            all_batches.extend(table_batches);
        });
        let table = geoarrow::table::Table::try_new(all_batches, output_schema)?;
        let (batches, schema) = table.into_inner();
        Ok(Table::new(schema, batches))
    }

    // TODO: reimplement this. Now that we have a vec of readers under the hood, we need to combine
    // multiple streams into one.
    //
    // #[wasm_bindgen]
    // pub fn read_stream(
    //     &self,
    //     options: JsValue,
    // ) -> WasmResult<wasm_streams::readable::sys::ReadableStream> {
    //     let options: Option<JsParquetReaderOptions> = serde_wasm_bindgen::from_value(options)?;
    //     let stream = self.inner.read_stream(options.unwrap_or_default().into())?;
    //     let out_stream = stream
    //         .map(|maybe_batch| {
    //             let batch = maybe_batch.map_err(JsError::from)?;
    //             let (schema, batches) = batch.into_inner();
    //             Ok(Table::new(schema, batches).into())
    //         })
    //         .boxed_local();
    //     Ok(wasm_streams::ReadableStream::from_stream(out_stream).into_raw())
    // }
}
