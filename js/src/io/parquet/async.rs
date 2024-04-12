use crate::error::WasmResult;
use crate::io::parquet::options::JsParquetReaderOptions;
use arrow_wasm::Table;
use futures::future::join_all;
use geoarrow::io::parquet::ParquetDataset as _ParquetDataset;
use geoarrow::io::parquet::ParquetFile as _ParquetFile;
use object_store::ObjectStore;
use object_store_wasm::http::HttpStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use std::sync::Arc;
use url::Url;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ParquetFile {
    file: _ParquetFile<ParquetObjectReader>,
}

#[wasm_bindgen]
impl ParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> WasmResult<ParquetFile> {
        let parsed_url = Url::parse(&url)?;
        let base_url = Url::parse(&parsed_url.origin().unicode_serialization())?;
        let storage_container = Arc::new(HttpStore::new(base_url));
        let location = object_store::path::Path::parse(parsed_url.path()).unwrap();
        let file_meta = storage_container.head(&location).await.unwrap();
        let reader = ParquetObjectReader::new(storage_container, file_meta);
        let file = _ParquetFile::new(reader).await?;
        Ok(Self { file })
    }

    /// The number of rows in this file.
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.file.num_rows()
    }

    /// The number of row groups in this file.
    #[wasm_bindgen(getter, js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.file.num_row_groups()
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
        let bbox = self.file.file_bbox(name)?;
        Ok(bbox.map(|b| b.to_vec()))
    }

    pub async fn read(&self, options: JsValue) -> WasmResult<Table> {
        let options: JsParquetReaderOptions = serde_wasm_bindgen::from_value(options)?;
        let table = self.file.read(options.into()).await?;
        let (schema, batches) = table.into_inner();
        Ok(Table::new(schema, batches))
    }

    #[wasm_bindgen(js_name = readRowGroups)]
    pub async fn read_row_groups(
        &self,
        row_groups: Vec<usize>,
        options: JsValue,
    ) -> WasmResult<Table> {
        let options: JsParquetReaderOptions = serde_wasm_bindgen::from_value(options)?;
        let table = self
            .file
            .read_row_groups(row_groups, options.into())
            .await?;
        let (schema, batches) = table.into_inner();
        Ok(Table::new(schema, batches))
    }
}

#[wasm_bindgen]
pub struct ParquetDataset {
    inner: _ParquetDataset<ParquetObjectReader>,
}

#[wasm_bindgen]
impl ParquetDataset {
    #[wasm_bindgen(constructor)]
    pub async fn new(urls: Vec<String>) -> WasmResult<ParquetDataset> {
        let readers: Vec<_> = urls
            .into_iter()
            .map(|url| async move {
                let parsed_url = Url::parse(&url).unwrap();
                let base_url = Url::parse(&parsed_url.origin().unicode_serialization()).unwrap();
                let storage_container = Arc::new(HttpStore::new(base_url));
                let location = object_store::path::Path::parse(parsed_url.path()).unwrap();
                let file_meta = storage_container.head(&location).await.unwrap();
                let reader = ParquetObjectReader::new(storage_container, file_meta);
                reader
            })
            .collect();
        let dataset = _ParquetDataset::new(join_all(readers).await).await?;
        Ok(Self { inner: dataset })
    }

    /// The total number of rows across all files.
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// The total number of row groups across all files
    #[wasm_bindgen(getter, js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.inner.num_row_groups()
    }
}
