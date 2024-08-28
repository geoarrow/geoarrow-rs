use geo::Rect;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::array::CoordType;
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{GeoParquetBboxCovering, GeoParquetMetadata};
use crate::io::parquet::reader::spatial_filter::{
    apply_bbox_row_filter, apply_bbox_row_groups, ParquetBboxStatistics,
};

/// Options for reading (Geo)Parquet
///
/// Geospatial options will only be applied if the target file has geospatial metadata.
#[derive(Clone, Default)]
pub struct GeoParquetReaderOptions {
    /// The number of rows in each batch. If not provided, the upstream [parquet] default is 1024.
    batch_size: Option<usize>,

    row_groups: Option<Vec<usize>>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_limit]
    limit: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_offset]
    offset: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_projection]
    mask: Option<ProjectionMask>,

    /// The GeoArrow coordinate type to use in the geometry arrays.
    ///
    /// Note that for now this is only used when parsing from WKB-encoded geometries.
    pub(crate) coord_type: CoordType,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    bbox: Option<Rect>,

    /// The paths in the Parquet schema to the bounding box columns. This will not be necessary as
    /// of GeoParquet 1.1.
    bbox_paths: Option<GeoParquetBboxCovering>,
}

impl GeoParquetReaderOptions {
    /// Set the size of [RecordBatch][arrow::array::RecordBatch] to produce.
    ///
    /// Defaults to 1024. If the batch_size more than the file row count, use the file row count.
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self {
            batch_size: Some(batch_size),
            ..self
        }
    }

    /// Only read data from the provided row group indexes
    ///
    /// This is also called row group filtering
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            mask: Some(mask),
            ..self
        }
    }

    /// Provide a limit to the number of rows to be read
    ///
    /// The limit will be applied after any Self::with_row_selection and Self::with_row_filter
    /// allowing it to limit the final set of rows decoded after any pushed down predicates
    ///
    /// It is recommended to enable reading the page index if using this functionality, to allow
    /// more efficient skipping over data pages. See [`ArrowReaderOptions::with_page_index`]
    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// Provide an offset to skip over the given number of rows
    ///
    /// The offset will be applied after any Self::with_row_selection and Self::with_row_filter
    /// allowing it to skip rows after any pushed down predicates
    ///
    /// It is recommended to enable reading the page index if using this functionality, to allow
    /// more efficient skipping over data pages. See ArrowReaderOptions::with_page_index
    pub fn with_offset(self, offset: usize) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    /// Set the GeoArrow [CoordType] to use when loading the input data.
    pub fn with_coord_type(self, coord_type: CoordType) -> Self {
        Self { coord_type, ..self }
    }

    /// Set the bounding box for reading with a spatial filter
    ///
    pub fn with_bbox(self, bbox: geo::Rect, bbox_paths: Option<GeoParquetBboxCovering>) -> Self {
        Self {
            bbox: Some(bbox),
            bbox_paths,
            ..self
        }
    }

    /// Apply these settings to an [ArrowReaderBuilder]
    pub(crate) fn apply_to_builder<T>(
        self,
        mut builder: ArrowReaderBuilder<T>,
        geo_meta: Option<&GeoParquetMetadata>,
    ) -> Result<ArrowReaderBuilder<T>> {
        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        if let Some(row_groups) = self.row_groups {
            builder = builder.with_row_groups(row_groups);
        }

        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }

        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }

        if let Some(mask) = self.mask {
            builder = builder.with_projection(mask);
        }

        if let (Some(bbox), bbox_paths) = (self.bbox, self.bbox_paths) {
            let bbox_paths = if let Some(paths) = bbox_paths {
                paths
            } else {
                let geo_meta = geo_meta
                    .as_ref()
                    .ok_or(GeoArrowError::General("No geospatial metadata".to_string()))?;
                geo_meta.bbox_covering(None)?.ok_or(GeoArrowError::General(
                    "No covering metadata found".to_string(),
                ))?
            };

            let bbox_cols = ParquetBboxStatistics::try_new(builder.parquet_schema(), &bbox_paths)?;
            builder = apply_bbox_row_groups(builder, &bbox_cols, bbox)?;
            builder = apply_bbox_row_filter(builder, bbox_cols, bbox)?;
        }

        Ok(builder)
    }
}
