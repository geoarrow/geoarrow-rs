use std::sync::Arc;

use crate::array::{CoordType, PolygonArray, RectBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{find_geoparquet_geom_columns, GeoParquetMetadata};
use crate::io::parquet::reader::options::ParquetReaderOptions;
use crate::io::parquet::reader::spatial_filter::{ParquetBboxPaths, ParquetBboxStatistics};
use crate::table::Table;

use arrow_schema::SchemaRef;
use futures::stream::TryStreamExt;
use geo::Rect;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::{FileMetaData, ParquetMetaData};
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

pub(crate) fn parse_table_geometries_to_native(
    table: &mut Table,
    metadata: &FileMetaData,
    coord_type: &CoordType,
) -> Result<()> {
    let geom_cols = find_geoparquet_geom_columns(metadata, table.schema(), *coord_type)?;
    geom_cols
        .iter()
        .try_for_each(|(geom_col_idx, target_geo_data_type)| {
            table.parse_geometry_to_native(*geom_col_idx, *target_geo_data_type)
        })
}

/// To create from an object-store item:
///
/// ```notest
/// let reader = ParquetObjectReader::new(store, meta);
///
/// ```
#[derive(Clone)]
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
}

#[derive(Clone)]
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
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::fs::File;

    #[tokio::test]
    async fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet")
            .await
            .unwrap();
        let options = Default::default();
        let _output_geotable = read_geoparquet_async(file, options).await.unwrap();
    }
}
