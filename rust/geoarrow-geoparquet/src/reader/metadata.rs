use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::{BoxType, CoordType, Dimension};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
#[cfg(feature = "async")]
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;
use parquet::schema::types::SchemaDescriptor;
use serde_json::Value;

use crate::metadata::{GeoParquetBboxCovering, GeoParquetMetadata};
use crate::reader::parse::infer_native_geoarrow_schema;
use crate::reader::spatial_filter::ParquetBboxStatistics;
use geoarrow_array::error::{GeoArrowError, Result};

/// An extension trait to DRY some code across the file and dataset metadata.
trait ArrowReaderMetadataExt {
    /// Access the [ArrowReaderMetadata] of this builder.
    fn reader_metadata(&self) -> &ArrowReaderMetadata;

    /// The number of rows in this file.
    fn num_rows(&self) -> usize {
        self.reader_metadata()
            .metadata()
            .row_groups()
            .iter()
            .fold(0, |acc, row_group_meta| {
                acc + usize::try_from(row_group_meta.num_rows()).unwrap()
            })
    }

    /// The number of row groups in this file.
    fn num_row_groups(&self) -> usize {
        self.reader_metadata().metadata().num_row_groups()
    }
}

impl ArrowReaderMetadataExt for ArrowReaderMetadata {
    fn reader_metadata(&self) -> &ArrowReaderMetadata {
        self
    }
}

/// The metadata necessary to construct a [`GeoParquetRecordBatchReaderBuilder`] or
/// [`GeoParquetRecordBatchStreamBuilder`].
///
/// This represents the metadata of a _single_ GeoParquet file. If you have a collection of
/// GeoParquet files representing a collective dataset with the same schema, use
/// [GeoParquetDatasetMetadata].
///
/// Note this structure is cheaply clone-able as it consists of several arcs.
///
/// This structure allows
///
/// 1. Loading metadata for a file once and then using that same metadata to construct multiple
///    separate readers, for example, to distribute readers across multiple threads
///
/// 2. Using a cached copy of the [`ParquetMetaData`] rather than reading it from the file each
///    time a reader is constructed.
#[derive(Debug, Clone)]
pub struct GeoParquetReaderMetadata {
    meta: ArrowReaderMetadata,
    geo_meta: Option<Arc<GeoParquetMetadata>>,
}

impl GeoParquetReaderMetadata {
    /// Construct a new [GeoParquetReaderMetadata] from [ArrowReaderMetadata]
    pub fn new(meta: ArrowReaderMetadata) -> Self {
        let geo_meta = GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata())
            .ok()
            .map(Arc::new);
        Self { meta, geo_meta }
    }

    /// Access the underlying [ArrowReaderMetadata].
    pub fn arrow_metadata(&self) -> &ArrowReaderMetadata {
        &self.meta
    }

    /// Access the geo metadata of this file.
    pub fn geo_metadata(&self) -> Option<&Arc<GeoParquetMetadata>> {
        self.geo_meta.as_ref()
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
    pub fn original_schema(&self) -> &SchemaRef {
        self.meta.schema()
    }

    /// Construct an _output_ Arrow schema based on the provided `CoordType`.
    ///
    /// E.g. when a GeoParquet file stores WKB in a binary column, we transform that column to a
    /// native representation when loading. This means that the Arrow schema of the _source_ is not
    /// the same as the schema of what gets loaded.
    pub fn resolved_schema(&self, coord_type: CoordType) -> Result<SchemaRef> {
        if let Some(geo_meta) = &self.geo_meta {
            infer_native_geoarrow_schema(self.meta.schema(), geo_meta, coord_type)
        } else {
            // If non-geospatial, return the same schema as output
            Ok(self.meta.schema().clone())
        }
    }

    /// The number of rows in this file.
    pub fn num_rows(&self) -> usize {
        self.meta.num_rows()
    }

    /// The number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.meta.num_row_groups()
    }

    /// Get the bounds of a single row group.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_group_bounds(
        &self,
        row_group_idx: usize,
        paths: Option<&GeoParquetBboxCovering>,
    ) -> Result<Option<geo_types::Rect>> {
        let paths = if let Some(paths) = paths {
            paths
        } else {
            let geo_meta = self
                .geo_meta
                .as_ref()
                .ok_or(GeoArrowError::General("No geospatial metadata".to_string()))?;
            &geo_meta.bbox_covering(None)?.ok_or(GeoArrowError::General(
                "No covering metadata found".to_string(),
            ))?
        };

        let geo_statistics = ParquetBboxStatistics::try_new(self.meta.parquet_schema(), paths)?;
        let row_group_meta = self.meta.metadata().row_group(row_group_idx);
        Ok(Some(geo_statistics.get_bbox(row_group_meta)?))
    }

    /// Get the bounds of all row groups.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_groups_bounds(&self, paths: Option<&GeoParquetBboxCovering>) -> Result<RectArray> {
        let paths = if let Some(paths) = paths {
            paths
        } else {
            let geo_meta = self
                .geo_meta
                .as_ref()
                .ok_or(GeoArrowError::General("No geospatial metadata".to_string()))?;
            &geo_meta.bbox_covering(None)?.ok_or(GeoArrowError::General(
                "No covering metadata found".to_string(),
            ))?
        };

        let geo_statistics = ParquetBboxStatistics::try_new(self.meta.parquet_schema(), paths)?;
        let rects = self
            .meta
            .metadata()
            .row_groups()
            .iter()
            .map(|rg_meta| geo_statistics.get_bbox(rg_meta))
            .collect::<Result<Vec<_>>>()?;
        let rect_type = BoxType::new(Dimension::XY, Default::default());
        let rect_array = RectBuilder::from_rects(rects.iter(), rect_type).finish();
        Ok(rect_array)
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

    /// Access the Coordinate Reference System (CRS) of the given column
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
}

impl From<ArrowReaderMetadata> for GeoParquetReaderMetadata {
    fn from(value: ArrowReaderMetadata) -> Self {
        Self::new(value)
    }
}

/// The metadata necessary to represent a collection of (Geo)Parquet files that share the same
/// schema.
///
/// If you have only one GeoParquet file, use [GeoParquetReaderMetadata].
///
/// Note this structure is cheaply clone-able as it consists of several arcs.
///
/// This structure allows
///
/// 1. Loading metadata for a file once and then using that same metadata to construct multiple
///    separate readers, for example, to distribute readers across multiple threads
///
/// 2. Using a cached copy of the [`ParquetMetaData`] rather than reading it from the file each
///    time a reader is constructed.
pub struct GeoParquetDatasetMetadata {
    files: HashMap<String, ArrowReaderMetadata>,
    geo_meta: Option<Arc<GeoParquetMetadata>>,
    schema: SchemaRef,
}

impl GeoParquetDatasetMetadata {
    /// Construct dataset metadata from a key-value map of [ArrowReaderMetadata].
    pub fn from_files(metas: HashMap<String, ArrowReaderMetadata>) -> Result<Self> {
        if metas.is_empty() {
            return Err(GeoArrowError::General("No files provided".to_string()));
        }

        let mut schema: Option<SchemaRef> = None;
        let mut geo_meta: Option<GeoParquetMetadata> = None;
        for meta in metas.values() {
            if let Some(ref _prior_schema) = schema {
                // TODO: check that schemas are equivalent
            } else {
                schema = Some(meta.schema().clone());
            }

            if let Some(geo_meta) = geo_meta.as_mut() {
                geo_meta.try_update(meta.metadata().file_metadata())?;
            } else {
                geo_meta = Some(GeoParquetMetadata::from_parquet_meta(
                    meta.metadata().file_metadata(),
                )?);
            }
        }

        Ok(Self {
            files: metas,
            schema: schema.unwrap(),
            geo_meta: geo_meta.map(Arc::new),
        })
    }

    /// Access the geo metadata of this file.
    pub fn geo_metadata(&self) -> Option<&Arc<GeoParquetMetadata>> {
        self.geo_meta.as_ref()
    }

    /// The total number of rows across all files.
    pub fn num_rows(&self) -> usize {
        self.files
            .values()
            .fold(0, |acc, file| acc + file.num_rows())
    }

    /// The total number of row groups across all files
    pub fn num_row_groups(&self) -> usize {
        self.files
            .values()
            .fold(0, |acc, file| acc + file.num_row_groups())
    }

    /// Construct an _output_ Arrow schema based on the provided `CoordType`.
    ///
    /// E.g. when a GeoParquet file stores WKB in a binary column, we transform that column to a
    /// native representation when loading. This means that the Arrow schema of the _source_ is not
    /// the same as the schema of what gets loaded.
    pub fn resolved_schema(&self, coord_type: CoordType) -> Result<SchemaRef> {
        if let Some(geo_meta) = &self.geo_meta {
            infer_native_geoarrow_schema(&self.schema, geo_meta, coord_type)
        } else {
            // If non-geospatial, return the same schema as output
            Ok(self.schema.clone())
        }
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

    /// Access the Coordinate Reference System (CRS) of the given column
    ///
    /// This is returned as a PROJJSON object. I.e. the variant returned should always be
    /// `Value::Object`.
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

    // /// Construct a collection of asynchronous [GeoParquetRecordBatchStreamBuilder] from this
    // /// dataset metadata
    // #[cfg(feature = "async")]
    // pub fn to_stream_builders<T: AsyncFileReader + Send + 'static, F>(
    //     &self,
    //     reader_cb: F,
    //     geo_options: GeoParquetReaderOptions,
    // ) -> Vec<GeoParquetRecordBatchStreamBuilder<T>>
    // where
    //     F: Fn(&str) -> T,
    // {
    //     self.files
    //         .iter()
    //         .map(|(path, arrow_meta)| {
    //             let reader = reader_cb(path);
    //             let file_metadata = GeoParquetReaderMetadata {
    //                 meta: arrow_meta.clone(),
    //                 geo_meta: self.geo_meta.clone(),
    //             };
    //             GeoParquetRecordBatchStreamBuilder::new_with_metadata_and_options(
    //                 reader,
    //                 file_metadata,
    //                 geo_options.clone(),
    //             )
    //         })
    //         .collect()
    // }

    // /// Construct a collection of synchronous [GeoParquetRecordBatchReaderBuilder] from this
    // /// dataset metadata
    // pub fn to_sync_builders<T: ChunkReader + 'static, F>(
    //     &self,
    //     reader_cb: F,
    //     geo_options: GeoParquetReaderOptions,
    // ) -> Vec<GeoParquetRecordBatchReaderBuilder<T>>
    // where
    //     F: Fn(&str) -> T,
    // {
    //     self.files
    //         .iter()
    //         .map(|(path, arrow_meta)| {
    //             let reader = reader_cb(path);
    //             let file_metadata = GeoParquetReaderMetadata {
    //                 meta: arrow_meta.clone(),
    //                 geo_meta: self.geo_meta.clone(),
    //             };
    //             GeoParquetRecordBatchReaderBuilder::new_with_metadata_and_options(
    //                 reader,
    //                 file_metadata,
    //                 geo_options.clone(),
    //             )
    //         })
    //         .collect()
    // }
}
