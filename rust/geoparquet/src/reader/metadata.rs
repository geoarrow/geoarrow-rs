use std::sync::Arc;

use arrow_schema::SchemaRef;
use geoarrow_array::array::RectArray;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{CoordType, Crs, Metadata};
use indexmap::IndexMap;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use crate::metadata::GeoParquetMetadata;
use crate::reader::parse::infer_geoarrow_schema;
use crate::reader::spatial_filter::ParquetBboxStatistics;

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
    geo_meta: Arc<GeoParquetMetadata>,
}

impl GeoParquetReaderMetadata {
    /// Construct a new [GeoParquetReaderMetadata] from an [ArrowReaderMetadata] and separate
    /// [GeoParquetMetadata].
    ///
    /// If you don't yet have a [GeoParquetMetadata], use
    /// [`from_arrow_meta`][Self::from_arrow_meta] instead.
    pub fn new(meta: ArrowReaderMetadata, geo_meta: GeoParquetMetadata) -> Self {
        Self {
            meta,
            geo_meta: Arc::new(geo_meta),
        }
    }

    /// Construct a new [GeoParquetReaderMetadata] from [ArrowReaderMetadata]
    pub fn from_arrow_meta(meta: ArrowReaderMetadata) -> GeoArrowResult<Self> {
        let geo_meta = if let Some(geo_meta) =
            GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata())
        {
            Arc::new(geo_meta?)
        } else {
            return Err(GeoArrowError::GeoParquet(
                "No `geo` key in Parquet metadata".to_string(),
            ));
        };
        Ok(Self { meta, geo_meta })
    }

    /// Access the underlying [ArrowReaderMetadata].
    pub fn arrow_metadata(&self) -> &ArrowReaderMetadata {
        &self.meta
    }

    /// Access the geo metadata of this file.
    pub fn geo_metadata(&self) -> &Arc<GeoParquetMetadata> {
        &self.geo_meta
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
    pub fn geoarrow_schema(
        &self,
        parse_to_native: bool,
        coord_type: CoordType,
    ) -> GeoArrowResult<SchemaRef> {
        infer_geoarrow_schema(
            self.meta.schema(),
            &self.geo_meta,
            parse_to_native,
            coord_type,
        )
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
        column_name: Option<&str>,
    ) -> GeoArrowResult<Option<geo_types::Rect>> {
        let (column_name, column_meta) = self.geo_meta.geometry_column(column_name)?;
        let bbox_covering =
            column_meta
                .bbox_covering(column_name)
                .ok_or(GeoArrowError::GeoParquet(format!(
                    "No covering metadata found for column: {}",
                    column_name
                )))?;
        let geo_statistics =
            ParquetBboxStatistics::try_new(self.meta.parquet_schema(), &bbox_covering)?;
        let row_group_meta = self.meta.metadata().row_group(row_group_idx);
        Ok(Some(geo_statistics.get_bbox(row_group_meta)?))
    }

    /// Get the bounds of all row groups.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_groups_bounds(&self, column_name: Option<&str>) -> GeoArrowResult<RectArray> {
        let (column_name, column_meta) = self.geo_meta.geometry_column(column_name)?;
        let bbox_covering =
            column_meta
                .bbox_covering(column_name)
                .ok_or(GeoArrowError::GeoParquet(format!(
                    "No covering metadata found for column: {}",
                    column_name
                )))?;

        let geo_statistics =
            ParquetBboxStatistics::try_new(self.meta.parquet_schema(), &bbox_covering)?;
        geo_statistics.get_bboxes(
            self.meta.metadata().row_groups(),
            Arc::new(column_meta.clone().into()),
        )
    }

    /// Access the bounding box of the given column for the entire file
    ///
    /// If no column name is passed, retrieves the bbox from the primary geometry column.
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    pub fn file_bbox<'a>(
        &'a self,
        column_name: Option<&'a str>,
    ) -> GeoArrowResult<Option<&'a [f64]>> {
        let (_, column_meta) = self.geo_meta.geometry_column(column_name)?;
        Ok(column_meta.bbox.as_deref())
    }

    /// Access the GeoArrow [`Metadata`] from the provided geometry column.
    pub fn geoarrow_metadata(&self, column_name: Option<&str>) -> GeoArrowResult<Metadata> {
        let (_, column_meta) = self.geo_meta.geometry_column(column_name)?;
        Ok(column_meta.clone().into())
    }

    /// Access the Coordinate Reference System (CRS) of the given column
    pub fn crs(&self, column_name: Option<&str>) -> GeoArrowResult<Crs> {
        let geoarrow_meta = self.geoarrow_metadata(column_name)?;
        Ok(geoarrow_meta.crs().clone())
    }
}

/// The metadata necessary to represent a collection of GeoParquet files that share the same
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
    files: IndexMap<String, ArrowReaderMetadata>,
    geo_meta: Arc<GeoParquetMetadata>,
    /// Raw schema from the Parquet file(s). This does not include GeoArrow metadata.
    schema: SchemaRef,
}

impl GeoParquetDatasetMetadata {
    /// Construct dataset metadata from a key-value map of [ArrowReaderMetadata].
    pub fn from_files(metas: IndexMap<String, ArrowReaderMetadata>) -> GeoArrowResult<Self> {
        if metas.is_empty() {
            return Err(GeoArrowError::GeoParquet("No files provided".to_string()));
        }

        let mut schema: Option<SchemaRef> = None;
        let mut geo_meta: Option<GeoParquetMetadata> = None;
        for meta in metas.values() {
            if let Some(_prior_schema) = &schema {
                // TODO: check that schemas are equivalent
            } else {
                schema = Some(meta.schema().clone());
            }

            if let Some(new_geo_meta) =
                GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata())
            {
                let new_geo_meta = new_geo_meta?;
                if let Some(geo_meta) = geo_meta.as_mut() {
                    geo_meta.try_update(&new_geo_meta)?;
                } else {
                    geo_meta = Some(new_geo_meta);
                }
            }
        }

        Ok(Self {
            files: metas,
            schema: schema.unwrap(),
            geo_meta: geo_meta
                .ok_or(GeoArrowError::GeoParquet(
                    "Expected GeoParquet dataset to have Geo metadata".to_string(),
                ))?
                .into(),
        })
    }

    /// Access underlying per-file metadata.
    pub fn files(&self) -> &IndexMap<String, ArrowReaderMetadata> {
        &self.files
    }

    /// Access the geo metadata of this file.
    pub fn geo_metadata(&self) -> &Arc<GeoParquetMetadata> {
        &self.geo_meta
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
    pub fn geoarrow_schema(
        &self,
        parse_to_native: bool,
        coord_type: CoordType,
    ) -> GeoArrowResult<SchemaRef> {
        infer_geoarrow_schema(&self.schema, &self.geo_meta, parse_to_native, coord_type)
    }

    /// Access the bounding box of the given column for the entire file
    ///
    /// If no column name is passed, retrieves the bbox from the primary geometry column.
    ///
    /// An Err will be returned if the column name does not exist in the dataset
    /// None will be returned if the metadata does not contain bounding box information.
    pub fn file_bbox<'a>(
        &'a self,
        column_name: Option<&'a str>,
    ) -> GeoArrowResult<Option<&'a [f64]>> {
        let (_, column_meta) = self.geo_meta.geometry_column(column_name)?;
        Ok(column_meta.bbox.as_deref())
    }

    /// Access the GeoArrow [`Metadata`] from the provided geometry column.
    pub fn geoarrow_metadata(&self, column_name: Option<&str>) -> GeoArrowResult<Metadata> {
        let (_, column_meta) = self.geo_meta.geometry_column(column_name)?;
        Ok(column_meta.clone().into())
    }

    /// Access the Coordinate Reference System (CRS) of the given column
    pub fn crs(&self, column_name: Option<&str>) -> GeoArrowResult<Crs> {
        let geoarrow_meta = self.geoarrow_metadata(column_name)?;
        Ok(geoarrow_meta.crs().clone())
    }
}
