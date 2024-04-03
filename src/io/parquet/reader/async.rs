use crate::array::{CoordType, PolygonArray, RectBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{build_arrow_schema, GeoParquetMetadata};
use crate::io::parquet::reader::spatial_filter::{
    apply_bbox_row_groups, ParquetBboxPaths, ParquetBboxStatistics,
};
use crate::io::parquet::reader::GeoParquetReaderOptions;
use crate::table::GeoTable;

use arrow_schema::SchemaRef;
use futures::stream::TryStreamExt;
use geo::Rect;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use serde_json::Value;

/// Asynchronously read a GeoParquet file to a GeoTable.
pub async fn read_geoparquet_async<R: AsyncFileReader + Unpin + Send + 'static>(
    reader: R,
    options: GeoParquetReaderOptions,
) -> Result<GeoTable> {
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_batch_size(options.batch_size);

    if let (Some(bbox_query), Some(bbox_paths)) = (options.bbox, options.bbox_paths) {
        let bbox_cols = ParquetBboxStatistics::try_new(builder.parquet_schema(), &bbox_paths)?;
        builder = apply_bbox_row_groups(builder, bbox_cols, bbox_query)?;
        // Need to fix the column ordering of the row filter inside construct_predicate
        // builder = apply_bbox_row_filter(builder, bbox_cols, bbox_query)?;
    }

    read_builder(builder, &options.coord_type).await
}

async fn read_builder<R: AsyncFileReader + Unpin + Send + 'static>(
    builder: ParquetRecordBatchStreamBuilder<R>,
    coord_type: &CoordType,
) -> Result<GeoTable> {
    let (arrow_schema, geometry_column_index, target_geo_data_type) =
        build_arrow_schema(&builder, coord_type)?;

    let stream = builder.build()?;
    let batches = stream.try_collect::<_>().await?;

    GeoTable::from_arrow(
        batches,
        arrow_schema,
        Some(geometry_column_index),
        target_geo_data_type,
    )
}

#[derive(Clone, Default)]
pub struct ParquetReaderOptions {
    batch_size: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    projection: Option<ProjectionMask>,
}

/// To create from an object-store item:
///
/// ```notest
/// let reader = ParquetObjectReader::new(store, meta);
///
/// ```
#[derive(Clone)]
pub struct ParquetFile<R: AsyncFileReader + Clone + Unpin + Send + 'static> {
    reader: R,
    meta: ArrowReaderMetadata,
    options: ParquetReaderOptions,
    geo_meta: Option<GeoParquetMetadata>,
}

impl<R: AsyncFileReader + Clone + Unpin + Send + 'static> ParquetFile<R> {
    /// Construct a new `ParquetFile` from a reader.
    ///
    /// This will fetch the metadata from the reader.
    pub async fn new(mut reader: R, options: ParquetReaderOptions) -> Result<Self> {
        let reader_options = ArrowReaderOptions::new().with_page_index(true);
        let meta = ArrowReaderMetadata::load_async(&mut reader, reader_options).await?;
        let geo_meta = GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata()).ok();
        Ok(Self {
            reader,
            meta,
            options,
            geo_meta,
        })
    }

    /// Construct a new `ParquetFile` from an existing metadata
    pub fn from_meta(
        reader: R,
        meta: ArrowReaderMetadata,
        options: ParquetReaderOptions,
    ) -> Result<Self> {
        let geo_meta = GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata()).ok();
        Ok(Self {
            reader,
            meta,
            options,
            geo_meta,
        })
    }

    /// The Arrow schema of the underlying data
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

    fn builder(
        &self,
        bbox: Option<Rect>,
        bbox_paths: Option<&ParquetBboxPaths>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            self.reader.clone(),
            self.meta.clone(),
        );

        if let Some(batch_size) = self.options.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        if let Some(limit) = self.options.limit {
            builder = builder.with_limit(limit);
        }

        if let Some(offset) = self.options.offset {
            builder = builder.with_offset(offset);
        }

        if let Some(projection) = &self.options.projection {
            builder = builder.with_projection(projection.clone());
        }

        if let (Some(bbox), Some(bbox_paths)) = (bbox, bbox_paths) {
            let bbox_cols = ParquetBboxStatistics::try_new(self.meta.parquet_schema(), bbox_paths)?;
            builder = apply_bbox_row_groups(builder, bbox_cols, bbox)?;
            // Need to fix the column ordering of the row filter inside construct_predicate
            // builder = apply_bbox_row_filter(builder, bbox_cols, bbox)?;
        }

        Ok(builder)
    }

    /// Read into a table.
    pub async fn read(
        &self,
        bbox: Option<Rect>,
        bbox_paths: Option<&ParquetBboxPaths>,
        coord_type: &CoordType,
    ) -> Result<GeoTable> {
        let builder = self.builder(bbox, bbox_paths)?;
        read_builder(builder, coord_type).await
    }

    /// Read the specified row groups into a table.
    pub async fn read_row_groups(
        &self,
        row_groups: Vec<usize>,
        coord_type: &CoordType,
    ) -> Result<GeoTable> {
        let builder = self
            .builder(None::<geo::Rect>, None)?
            .with_row_groups(row_groups);
        read_builder(builder, coord_type).await
    }
}

#[derive(Clone)]
pub struct ParquetDataset<R: AsyncFileReader + Clone + Unpin + Send + 'static> {
    // TODO: should this be a hashmap instead?
    files: Vec<ParquetFile<R>>,
}

impl<R: AsyncFileReader + Clone + Unpin + Send + 'static> ParquetDataset<R> {
    pub async fn new(readers: Vec<R>, options: ParquetReaderOptions) -> Result<Self> {
        let futures = readers
            .into_iter()
            .map(|reader| ParquetFile::new(reader, options.clone()));
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
    pub async fn read(
        &self,
        bbox: Option<Rect>,
        bbox_paths: Option<&ParquetBboxPaths>,
        coord_type: &CoordType,
    ) -> Result<GeoTable> {
        let futures = self
            .files
            .iter()
            .map(|file| file.read(bbox, bbox_paths, coord_type));
        let tables = futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let geometry_column_index = tables[0].geometry_column_index();
        let schema = tables[0].schema().clone();
        let batches = tables
            .into_iter()
            .flat_map(|table| {
                if !table.is_empty() {
                    table.batches().clone()
                } else {
                    vec![]
                }
            })
            .collect();
        GeoTable::try_new(schema, batches, geometry_column_index)
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
