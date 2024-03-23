use geoarrow::array::CoordType;
use geoarrow::io::parquet::ParquetDataset as _ParquetDataset;
use geoarrow::io::parquet::ParquetFile as _ParquetFile;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
use crate::io::parquet::async_file_reader::HTTPFileReader;
use crate::table::GeoTable;

#[wasm_bindgen]
pub struct ParquetFile {
    file: _ParquetFile<HTTPFileReader>,
}

#[wasm_bindgen]
impl ParquetFile {
    #[wasm_bindgen(constructor)]
    pub async fn new(url: String) -> WasmResult<ParquetFile> {
        let reader = HTTPFileReader::new(url, Default::default(), 500_000);
        let file = _ParquetFile::new(reader, Default::default()).await?;
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

    pub async fn read(&self) -> WasmResult<GeoTable> {
        let table = self.file.read(&Default::default()).await?;
        Ok(table.into())
    }

    #[wasm_bindgen(js_name = readRowGroups)]
    pub async fn read_row_groups(&self, row_groups: Vec<usize>) -> WasmResult<GeoTable> {
        let table = self
            .file
            .read_row_groups(row_groups, &CoordType::Interleaved)
            .await?;
        Ok(table.into())
    }
}

#[wasm_bindgen]
pub struct ParquetDataset {
    inner: _ParquetDataset<HTTPFileReader>,
}

#[wasm_bindgen]
impl ParquetDataset {
    #[wasm_bindgen(constructor)]
    pub async fn new(urls: Vec<String>) -> WasmResult<ParquetDataset> {
        let readers = urls
            .into_iter()
            .map(|url| HTTPFileReader::new(url, Default::default(), 500_000))
            .collect();
        let dataset = _ParquetDataset::new(readers, Default::default()).await?;
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
