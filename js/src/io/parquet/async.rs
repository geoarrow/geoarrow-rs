// parquet 59 deprecates ParquetObjectReader (arrow-rs#10308); it stays the
// right reader over the wasm HTTP object store.
#![allow(deprecated)]

use std::sync::Arc;

use arrow_wasm::Table;
use futures::TryStreamExt;
use futures::future::join_all;
use geo::Rect;
use geoarrow_schema::CoordType;
use geoparquet::reader::{
    GeoParquetDatasetMetadata, GeoParquetReaderBuilder, GeoParquetReaderMetadata,
    GeoParquetRecordBatchStream,
};
use indexmap::IndexMap;
use object_store::ObjectStore;
use object_store::http::HttpBuilder;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use url::Url;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;

/// The spec gives four values, or six for a 3D file. An inverted box errors:
/// `geo::Rect::new` would swap its corners without a word and give the
/// complement of the region asked for.
fn bbox_rect(bbox: &Option<Vec<f64>>) -> WasmResult<Option<Rect>> {
    let Some(b) = bbox.as_ref() else {
        return Ok(None);
    };
    let (xmin, ymin, xmax, ymax) = match b.len() {
        4 => (b[0], b[1], b[2], b[3]),
        6 => (b[0], b[1], b[3], b[4]),
        n => {
            return Err(JsError::new(&format!(
                "bbox must have 4 elements [xmin, ymin, xmax, ymax] or 6 elements \
                 [xmin, ymin, zmin, xmax, ymax, zmax]; got {n}"
            )));
        }
    };
    if xmin > xmax || ymin > ymax {
        return Err(JsError::new(
            "inverted bbox (xmin > xmax or ymin > ymax) is not supported; \
             antimeridian-crossing bboxes must be split into two queries by the caller",
        ));
    }
    Ok(Some(Rect::new(
        geo::coord! { x: xmin, y: ymin },
        geo::coord! { x: xmax, y: ymax },
    )))
}

/// Reject the parts of a URL that the HTTP store drops. It keeps the origin and
/// the path only, thus a query parameter or a credential would go missing and
/// every request would fail as an opaque 403 or 404.
fn reject_unsupported_url_parts(url: &Url) -> WasmResult<()> {
    if url.query().is_some_and(|q| !q.is_empty())
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(JsError::new(
            "URL query parameters and credentials are not supported; the store fetches by \
             origin + path only. Pass a plain URL (presigned URLs will not work).",
        ));
    }
    Ok(())
}

/// A remote GeoParquet file, read over HTTP byte-range requests
///
/// The footer is fetched once and held, thus `read` pays for the row group bytes
/// alone.
#[wasm_bindgen]
pub struct ParquetFile {
    path: Path,
    geoparquet_meta: GeoParquetReaderMetadata,
    store: Arc<dyn ObjectStore>,
}

#[wasm_bindgen]
impl ParquetFile {
    /// Open a remote GeoParquet file and hold its metadata
    // Not a constructor: wasm-bindgen emits invalid TypeScript for an async one.
    #[wasm_bindgen(js_name = open)]
    pub async fn open(url: String) -> WasmResult<ParquetFile> {
        let parsed_url = Url::parse(&url)?;
        reject_unsupported_url_parts(&parsed_url)?;
        let base_url = parsed_url.origin().unicode_serialization();
        let store: Arc<dyn ObjectStore> = Arc::new(HttpBuilder::new().with_url(base_url).build()?);
        // `from_url_path` percent-decodes; `Path::parse` would leave the path
        // encoded for the HTTP client to encode again, so `my%20file` would be
        // fetched as `my%2520file`.
        let path = Path::from_url_path(parsed_url.path())?;
        let mut reader = ParquetObjectReader::new(store.clone(), path.clone());
        let arrow_meta = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        let geoparquet_meta = GeoParquetReaderMetadata::from_arrow_meta(arrow_meta)?;
        Ok(Self {
            path,
            geoparquet_meta,
            store,
        })
    }

    /// Note that a malformed footer that reports a negative count throws
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> WasmResult<usize> {
        Ok(self.geoparquet_meta.num_rows()?)
    }

    #[wasm_bindgen(getter, js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.geoparquet_meta.num_row_groups()
    }

    /// The bounding box of a column across the whole file, as
    /// `[xmin, ymin, xmax, ymax]`. `None` takes the primary geometry column
    #[wasm_bindgen(js_name = fileBbox)]
    pub fn file_bbox(&self, column_name: Option<String>) -> WasmResult<Option<Vec<f64>>> {
        let bbox = self.geoparquet_meta.file_bbox(column_name.as_deref())?;
        Ok(bbox.map(|b| b.to_vec()))
    }

    /// Read the file into an Arrow Table, with each geometry column in its
    /// native GeoArrow type. Filtering by `bbox` needs the covering metadata
    /// that `writeGeoParquet` writes on its `generateCovering` option.
    #[wasm_bindgen]
    pub async fn read(&self, bbox: Option<Vec<f64>>) -> WasmResult<Table> {
        let reader = ParquetObjectReader::new(self.store.clone(), self.path.clone());
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            reader,
            self.geoparquet_meta.arrow_metadata().clone(),
        );
        if let Some(rect) = bbox_rect(&bbox)? {
            let geo_meta = self.geoparquet_meta.geo_metadata().clone();
            builder = builder.with_intersecting_row_groups(rect, &geo_meta, None)?;
            builder = builder.with_intersecting_row_filter(rect, &geo_meta, None)?;
        }
        let target_schema = self
            .geoparquet_meta
            .geoarrow_schema(true, CoordType::Interleaved)?;
        let parquet_stream = builder.build()?;
        let geo_stream =
            GeoParquetRecordBatchStream::try_new(parquet_stream, target_schema.clone())?;
        let batches: Vec<_> = geo_stream.try_collect::<Vec<_>>().await?;
        Ok(Table::new(target_schema, batches))
    }
}

/// A remote GeoParquet dataset of many fragment files, read over HTTP
/// byte-range requests into one Arrow Table
#[wasm_bindgen]
pub struct ParquetDataset {
    meta: GeoParquetDatasetMetadata,
    /// Decoded once at `open`: a second pass through `Path::from` would encode
    /// the `%` again and fetch the wrong URL.
    paths: IndexMap<String, Path>,
    store: Arc<dyn ObjectStore>,
}

#[wasm_bindgen]
impl ParquetDataset {
    /// Open a dataset at `root_url` from a list of fragment paths, each relative
    /// to that root and encoded as it appears in a URL
    #[wasm_bindgen(js_name = open)]
    pub async fn open(root_url: String, fragment_paths: Vec<String>) -> WasmResult<ParquetDataset> {
        let parsed_root = Url::parse(&root_url)?;
        reject_unsupported_url_parts(&parsed_root)?;
        let store: Arc<dyn ObjectStore> = Arc::new(HttpBuilder::new().with_url(root_url).build()?);
        let mut paths: IndexMap<String, Path> = IndexMap::new();
        for p in &fragment_paths {
            paths.insert(p.clone(), Path::from_url_path(p)?);
        }
        let meta_futures = paths.values().map(|path| {
            let store = store.clone();
            let path = path.clone();
            async move {
                let mut reader = ParquetObjectReader::new(store, path);
                ArrowReaderMetadata::load_async(&mut reader, Default::default()).await
            }
        });
        let arrow_metas = join_all(meta_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let mut metas: IndexMap<String, ArrowReaderMetadata> = IndexMap::new();
        for (key, arrow_meta) in paths.keys().zip(arrow_metas) {
            metas.insert(key.clone(), arrow_meta);
        }
        Ok(Self {
            meta: GeoParquetDatasetMetadata::from_files(metas)?,
            paths,
            store,
        })
    }

    /// Note that a malformed footer that reports a negative count throws
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> WasmResult<usize> {
        Ok(self.meta.num_rows()?)
    }

    #[wasm_bindgen(getter, js_name = numRowGroups)]
    pub fn num_row_groups(&self) -> usize {
        self.meta.num_row_groups()
    }

    /// Read every fragment into one Arrow Table under the unified schema of the
    /// dataset
    ///
    /// `bbox` keeps rows fragment by fragment, as in [`ParquetFile::read`]
    #[wasm_bindgen]
    pub async fn read(&self, bbox: Option<Vec<f64>>) -> WasmResult<Table> {
        let target_schema = self.meta.geoarrow_schema(true, CoordType::Interleaved)?;
        let rect = bbox_rect(&bbox)?;
        let geo_meta = self.meta.geo_metadata().clone();

        let mut streams = Vec::with_capacity(self.meta.files().len());
        for (key, arrow_meta) in self.meta.files() {
            let path = self
                .paths
                .get(key)
                .ok_or_else(|| JsError::new("internal: fragment path missing from path map"))?
                .clone();
            let reader = ParquetObjectReader::new(self.store.clone(), path);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta.clone());
            if let Some(rect) = rect {
                builder = builder.with_intersecting_row_groups(rect, &geo_meta, None)?;
                builder = builder.with_intersecting_row_filter(rect, &geo_meta, None)?;
            }
            let parquet_stream = builder.build()?;
            let geo_stream =
                GeoParquetRecordBatchStream::try_new(parquet_stream, target_schema.clone())?;
            streams.push(geo_stream.try_collect::<Vec<_>>());
        }

        let mut all_batches = Vec::new();
        for batches in join_all(streams).await {
            all_batches.extend(batches?);
        }
        Ok(Table::new(target_schema, all_batches))
    }
}
