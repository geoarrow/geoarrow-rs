use arrow_wasm::Table;
use geo::coord;
use geoarrow::array::CoordType;
use geoarrow::io::parquet::ParquetDataset as _ParquetDataset;
use geoarrow::io::parquet::ParquetFile as _ParquetFile;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
use crate::io::parquet::async_file_reader::HTTPFileReader;

#[wasm_bindgen]
pub struct GeoParquetBboxPaths {
    minx_path: Vec<String>,
    miny_path: Vec<String>,
    maxx_path: Vec<String>,
    maxy_path: Vec<String>,
}

#[wasm_bindgen]
impl GeoParquetBboxPaths {
    #[wasm_bindgen(constructor)]
    pub fn new(
        minx_path: Vec<String>,
        miny_path: Vec<String>,
        maxx_path: Vec<String>,
        maxy_path: Vec<String>,
    ) -> GeoParquetBboxPaths {
        GeoParquetBboxPaths {
            minx_path,
            miny_path,
            maxx_path,
            maxy_path,
        }
    }
}

impl From<GeoParquetBboxPaths> for geoarrow::io::parquet::ParquetBboxPaths {
    fn from(value: GeoParquetBboxPaths) -> Self {
        Self {
            minx_path: value.minx_path,
            miny_path: value.miny_path,
            maxx_path: value.maxx_path,
            maxy_path: value.maxy_path,
        }
    }
}

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

    pub async fn read(&self) -> WasmResult<Table> {
        let table = self.file.read(None, None, &Default::default()).await?;
        let (schema, batches) = table.into_inner();
        Ok(Table::new(schema, batches))
    }

    #[wasm_bindgen(js_name = readRowGroups)]
    pub async fn read_row_groups(&self, row_groups: Vec<usize>) -> WasmResult<Table> {
        let table = self
            .file
            .read_row_groups(row_groups, &CoordType::Interleaved)
            .await?;
        let (schema, batches) = table.into_inner();
        Ok(Table::new(schema, batches))
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

    /// Read this entire file in an async fashion.
    #[wasm_bindgen]
    pub async fn read(
        &self,
        bbox: Option<Vec<f64>>,
        bbox_paths: Option<GeoParquetBboxPaths>,
    ) -> WasmResult<Table> {
        let inner = self.inner.clone();
        let bbox_paths = bbox_paths.map(geoarrow::io::parquet::ParquetBboxPaths::from);
        let bbox = bbox.map(|item| {
            geo::Rect::new(
                coord! {x: item[0], y: item[1]},
                coord! {x: item[2], y: item[3]},
            )
        });
        let table = inner
            .read(bbox, bbox_paths.as_ref(), &CoordType::Interleaved)
            .await?;
        let (schema, batches) = table.into_inner();
        Ok(Table::new(schema, batches))
    }
}
