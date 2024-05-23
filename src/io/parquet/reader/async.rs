use std::sync::Arc;

use crate::array::{PolygonArray, RectBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::GeoParquetMetadata;
use crate::io::parquet::reader::options::ParquetReaderOptions;
use crate::io::parquet::reader::parse_table_geometries_to_native;
use crate::io::parquet::reader::spatial_filter::{ParquetBboxPaths, ParquetBboxStatistics};
use crate::table::Table;

use arrow_schema::SchemaRef;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use geo::Rect;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use serde_json::Value;

/// Asynchronously read a GeoParquet file to a Table.
pub async fn read_geoparquet_async<R: AsyncFileReader + Unpin + Send + 'static>(
    reader: R,
    options: ParquetReaderOptions,
) -> Result<Table> {
    let file = ParquetFile::new(reader).await?;
    let builder = file.builder(options)?;
    read_builder(builder).await
}

async fn read_builder<R: AsyncFileReader + Unpin + Send + 'static>(
    builder: ParquetRecordBatchStreamBuilder<R>,
) -> Result<Table> {
    let arrow_schema = builder.schema().clone();

    let stream = builder.build()?;
    let batches = stream.try_collect::<_>().await?;

    Table::try_new(arrow_schema, batches)
}

fn read_stream_dataset<R: AsyncFileReader + Unpin + Clone + Send + 'static>(
    files: Vec<ParquetFile<R>>,
    options: ParquetReaderOptions,
) -> Result<BoxStream<'static, Result<Table>>> {
    let stream = futures::stream::iter(files)
        .flat_map(move |file| file.read_stream(options.clone()).unwrap())
        .boxed();
    Ok(stream)
}

/// To create from an object-store item:
///
/// ```notest
/// let reader = ParquetObjectReader::new(store, meta);
///
/// ```
#[derive(Debug, Clone)]
pub struct ParquetFile<R: AsyncFileReader + Unpin + Send + 'static> {
    reader: R,
    meta: ArrowReaderMetadata,
    geo_meta: Option<GeoParquetMetadata>,
}

impl<R: AsyncFileReader + Unpin + Send + 'static> ParquetFile<R> {
    /// Construct a new `ParquetFile` from a reader.
    ///
    /// This will fetch the metadata from the reader.
    pub async fn new(mut reader: R) -> Result<Self> {
        let reader_options = ArrowReaderOptions::new().with_page_index(true);
        let meta = ArrowReaderMetadata::load_async(&mut reader, reader_options).await?;
        let geo_meta = GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata()).ok();
        Ok(Self {
            reader,
            meta,
            geo_meta,
        })
    }

    /// Construct a new `ParquetFile` from an existing metadata
    pub fn from_meta(reader: R, meta: ArrowReaderMetadata) -> Result<Self> {
        let geo_meta = GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata()).ok();
        Ok(Self {
            reader,
            meta,
            geo_meta,
        })
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        self.meta.metadata()
    }

    /// Returns the parquet [`SchemaDescriptor`] for this parquet file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.meta.parquet_schema()
    }

    /// Returns the Arrow [`SchemaRef`] of the underlying data
    ///
    /// Note that this schema is before conversion of any geometry column(s) to GeoArrow.
    pub fn schema(&self) -> SchemaRef {
        self.meta.schema().clone()
    }

    /// The number of rows in this file.
    pub fn num_rows(&self) -> usize {
        self.meta
            .metadata()
            .row_groups()
            .iter()
            .fold(0, |acc, row_group_meta| {
                acc + usize::try_from(row_group_meta.num_rows()).unwrap()
            })
    }

    /// The number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.meta.metadata().num_row_groups()
    }

    /// Access the geo metadata of this file.
    pub fn geo_metadata(&self) -> Option<&GeoParquetMetadata> {
        self.geo_meta.as_ref()
    }

    /// Get the bounds of a single row group.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_group_bounds(
        &self,
        paths: &ParquetBboxPaths,
        row_group_idx: usize,
    ) -> Result<Option<Rect>> {
        let geo_statistics = ParquetBboxStatistics::try_new(self.meta.parquet_schema(), paths)?;
        let row_group_meta = self.meta.metadata().row_group(row_group_idx);
        Ok(Some(geo_statistics.get_bbox(row_group_meta)?))
    }

    /// Get the bounds of all row groups.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_groups_bounds(&self, paths: &ParquetBboxPaths) -> Result<PolygonArray<i32>> {
        let geo_statistics = ParquetBboxStatistics::try_new(self.meta.parquet_schema(), paths)?;
        let rects = self
            .meta
            .metadata()
            .row_groups()
            .iter()
            .map(|rg_meta| geo_statistics.get_bbox(rg_meta))
            .collect::<Result<Vec<_>>>()?;
        let rect_array = RectBuilder::from_rects(rects.iter(), Default::default()).finish();
        Ok(rect_array.into())
    }

    /// Access the bounding box of the given column for the entire file
    ///
    /// If no column name is passed, retrieves the bbox from the primary geometry column.
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    pub fn file_bbox(&self, column_name: Option<&str>) -> Result<Option<&[f64]>> {
        if let Some(geo_meta) = self.geo_metadata() {
            let column_name = column_name.unwrap_or(geo_meta.primary_column.as_str());
            let column_meta = geo_meta
                .columns
                .get(column_name)
                .ok_or(GeoArrowError::General(format!(
                    "Column {} not found in GeoParquet metadata",
                    column_name
                )))?;
            Ok(column_meta.bbox.as_deref())
        } else {
            Ok(None)
        }
    }

    pub fn crs(&self, column_name: Option<&str>) -> Result<Option<&Value>> {
        if let Some(geo_meta) = self.geo_metadata() {
            let column_name = column_name.unwrap_or(geo_meta.primary_column.as_str());
            let column_meta = geo_meta
                .columns
                .get(column_name)
                .ok_or(GeoArrowError::General(format!(
                    "Column {} not found in GeoParquet metadata",
                    column_name
                )))?;
            Ok(column_meta.crs.as_ref())
        } else {
            Ok(None)
        }
    }

    fn builder(self, options: ParquetReaderOptions) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(self.reader, self.meta.clone());
        options.apply_to_builder(builder)
    }
}

impl<R: AsyncFileReader + Unpin + Clone + Send + 'static> ParquetFile<R> {
    /// Read Parquet into Arrow without parsing geometries into native representation
    async fn _read(&self, options: ParquetReaderOptions) -> Result<Table> {
        let builder = self.clone().builder(options)?;
        read_builder(builder).await
    }

    /// Read into a table.
    pub async fn read(&self, options: ParquetReaderOptions) -> Result<Table> {
        let coord_type = options.coord_type;
        let mut table = self._read(options).await?;
        parse_table_geometries_to_native(&mut table, self.metadata().file_metadata(), &coord_type)?;
        Ok(table)
    }

    /// Read the specified row groups into a table.
    pub async fn read_row_groups(
        &self,
        row_groups: Vec<usize>,
        options: ParquetReaderOptions,
    ) -> Result<Table> {
        let coord_type = options.coord_type;
        let builder = self.clone().builder(options)?.with_row_groups(row_groups);
        let mut table = read_builder(builder).await?;
        parse_table_geometries_to_native(&mut table, self.metadata().file_metadata(), &coord_type)?;
        Ok(table)
    }

    pub fn read_stream(
        &self,
        options: ParquetReaderOptions,
    ) -> Result<BoxStream<'static, Result<Table>>> {
        let coord_type = options.coord_type;
        let builder = self.clone().builder(options)?;
        let arrow_schema = builder.schema().clone();
        let parquet_file_metadata = self.metadata().file_metadata().clone();
        let stream = builder.build()?;
        let out_stream = stream
            .map(move |maybe_batch| {
                let batch = maybe_batch.unwrap();
                let mut table = Table::try_new(arrow_schema.clone(), vec![batch])?;
                parse_table_geometries_to_native(&mut table, &parquet_file_metadata, &coord_type)?;
                Ok(table)
            })
            .boxed();
        Ok(out_stream)
    }
}

#[derive(Debug, Clone)]
pub struct ParquetDataset<R: AsyncFileReader + Clone + Unpin + Send + 'static> {
    // TODO: should this be a hashmap instead?
    files: Vec<ParquetFile<R>>,
}

impl<R: AsyncFileReader + Clone + Unpin + Send + 'static> ParquetDataset<R> {
    pub async fn new(readers: Vec<R>) -> Result<Self> {
        if readers.is_empty() {
            return Err(GeoArrowError::General(
                "Must pass at least one file to ParquetDataset::new".to_string(),
            ));
        }

        let futures = readers.into_iter().map(|reader| ParquetFile::new(reader));
        let files = futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Validate metadata across files with `GeoParquetMetadata::try_compatible_with`
        for pair in files.windows(2) {
            match (pair[0].geo_metadata(), pair[1].geo_metadata()) {
                (Some(left), Some(right)) => left.try_compatible_with(right)?,
                (None, Some(_)) | (Some(_), None) => {
                    return Err(GeoArrowError::General(
                        "Not all files have GeoParquet metadata".to_string(),
                    ))
                }
                (None, None) => (),
            }
        }

        Ok(Self { files })
    }

    /// The total number of rows across all files.
    pub fn num_rows(&self) -> usize {
        self.files.iter().fold(0, |acc, file| acc + file.num_rows())
    }

    /// The total number of row groups across all files
    pub fn num_row_groups(&self) -> usize {
        self.files
            .iter()
            .fold(0, |acc, file| acc + file.num_row_groups())
    }

    /// The total bounds of the entire dataset
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    pub fn total_bounds(&self, _column_name: Option<&str>) -> Result<Option<Vec<f64>>> {
        // let x = self.files.iter().try_fold(None::<Vec<f64>>, |acc, file| {
        //     match (acc, file.file_bbox(column_name)?) {
        //         (None, None) => Ok(None),
        //         (Some(acc), None)
        //     }
        // })?;
        todo!()
    }

    /// Read into a table.
    pub async fn read(&self, options: ParquetReaderOptions) -> Result<Table> {
        // We first read all the tables **without** parsing geometry columns into a native
        // representation.
        let futures = self.files.iter().map(|file| file._read(options.clone()));
        let tables = futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let schema = tables[0].schema().clone();
        let batches = tables
            .into_iter()
            .flat_map(|table| {
                if !table.is_empty() {
                    table.batches().to_vec()
                } else {
                    vec![]
                }
            })
            .collect();

        // Then after reading data directly, we parse all geometry columns to a native
        // representation
        let mut table = Table::try_new(schema, batches)?;

        if table.is_empty() {
            return Ok(table);
        }

        let parquet_file_metadata = self.files[0].metadata().file_metadata();
        parse_table_geometries_to_native(&mut table, parquet_file_metadata, &options.coord_type)?;
        Ok(table)
    }

    pub fn read_stream(
        &self,
        options: ParquetReaderOptions,
    ) -> Result<BoxStream<'static, Result<Table>>> {
        read_stream_dataset(self.files.clone(), options)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::future::join_all;
    use geo::coord;
    use object_store::http::HttpBuilder;
    use object_store::ObjectStore;
    use parquet::arrow::async_reader::ParquetObjectReader;
    use tokio::fs::File;
    use url::Url;

    #[tokio::test]
    async fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet")
            .await
            .unwrap();
        let options = Default::default();
        let _output_geotable = read_geoparquet_async(file, options).await.unwrap();
    }

    #[ignore = "don't run overture HTTP test on CI"]
    #[tokio::test]
    async fn overture() {
        let urls = vec![
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00000-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00001-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00002-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00003-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00004-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00005-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00006-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00007-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00008-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00009-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00010-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00011-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00012-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00013-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00014-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00015-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00016-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00017-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00018-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00019-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00020-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00021-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00022-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00023-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00024-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00025-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00026-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00027-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00028-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00029-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00030-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00031-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00032-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00033-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00034-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00035-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00036-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00037-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00038-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00039-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00040-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00041-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00042-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00043-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00044-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00045-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00046-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00047-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00048-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00049-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00050-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00051-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00052-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00053-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00054-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00055-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00056-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00057-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00058-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00059-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00060-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00061-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00062-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00063-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00064-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00065-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00066-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00067-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00068-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00069-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00070-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00071-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00072-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00073-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00074-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00075-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00076-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00077-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00078-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00079-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00080-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00081-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00082-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00083-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00084-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00085-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00086-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00087-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00088-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00089-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00090-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00091-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00092-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00093-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00094-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00095-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00096-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00097-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00098-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00099-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00100-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00101-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00102-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00103-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00104-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00105-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00106-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00107-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00108-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00109-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00110-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00111-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00112-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00113-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00114-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00115-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00116-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00117-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00118-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00119-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00120-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00121-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00122-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00123-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00124-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00125-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00126-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00127-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00128-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00129-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00130-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00131-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00132-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00133-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00134-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00135-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00136-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00137-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00138-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00139-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00140-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00141-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00142-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00143-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00144-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00145-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00146-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00147-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00148-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00149-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00150-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00151-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00152-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00153-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00154-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00155-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00156-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00157-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00158-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00159-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00160-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00161-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00162-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00163-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00164-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00165-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00166-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00167-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00168-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00169-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00170-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00171-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00172-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00173-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00174-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00175-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00176-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00177-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00178-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00179-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00180-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00181-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00182-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00183-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00184-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00185-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00186-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00187-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00188-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00189-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00190-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00191-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00192-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00193-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00194-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00195-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00196-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00197-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00198-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00199-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00200-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00201-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00202-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00203-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00204-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00205-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00206-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00207-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00208-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00209-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00210-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00211-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00212-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00213-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00214-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00215-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00216-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00217-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00218-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00219-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00220-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00221-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00222-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00223-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00224-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00225-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00226-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00227-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00228-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00229-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00230-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
"https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00231-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
        ];

        let readers: Vec<_> = urls
            .into_iter()
            .map(|url| async move {
                let parsed_url = Url::parse(url).unwrap();
                let base_url = Url::parse(&parsed_url.origin().unicode_serialization()).unwrap();
                let path = object_store::path::Path::parse(parsed_url.path()).unwrap();
                let store = HttpBuilder::new().with_url(base_url).build().unwrap();
                let file_meta = store.head(&path).await.unwrap();
                ParquetObjectReader::new(Arc::new(store), file_meta)
            })
            .collect();
        let dataset = ParquetDataset::new(join_all(readers).await).await.unwrap();

        let bbox_paths = ParquetBboxPaths {
            minx_path: vec!["bbox".to_string(), "minx".to_string()],
            miny_path: vec!["bbox".to_string(), "miny".to_string()],
            maxx_path: vec!["bbox".to_string(), "maxx".to_string()],
            maxy_path: vec!["bbox".to_string(), "maxy".to_string()],
        };
        let c1 = coord! { x: 94.9218037, y: 26.7301782 };
        let c2 = coord! {x: 94.9618037, y: 26.7501782};
        let rect = geo::Rect::new(c1, c2);

        let options = ParquetReaderOptions {
            bbox: Some(rect),
            bbox_paths: Some(bbox_paths),
            ..Default::default()
        };

        let table = dataset.read(options).await.unwrap();
        dbg!(table.is_empty());
        dbg!(table.len());
    }
}
