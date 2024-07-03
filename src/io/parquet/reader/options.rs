use geo::Rect;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::parquet::reader::spatial_filter::{
    apply_bbox_row_filter, apply_bbox_row_groups, ParquetBboxPaths, ParquetBboxStatistics,
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
    coord_type: CoordType,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    bbox: Option<Rect>,

    /// The paths in the Parquet schema to the bounding box columns. This will not be necessary as
    /// of GeoParquet 1.1.
    bbox_paths: Option<ParquetBboxPaths>,
}

impl GeoParquetReaderOptions {
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self {
            batch_size: Some(batch_size),
            ..self
        }
    }

    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            mask: Some(mask),
            ..self
        }
    }

    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub fn with_offset(self, offset: usize) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub fn with_coord_type(self, coord_type: CoordType) -> Self {
        Self { coord_type, ..self }
    }

    pub fn with_bbox(self, bbox: geo::Rect, bbox_paths: ParquetBboxPaths) -> Self {
        Self {
            bbox: Some(bbox),
            bbox_paths: Some(bbox_paths),
            ..self
        }
    }

    pub(crate) fn apply_to_builder<T>(
        self,
        mut builder: ArrowReaderBuilder<T>,
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

        if let (Some(bbox), Some(bbox_paths)) = (self.bbox, self.bbox_paths) {
            let bbox_cols = ParquetBboxStatistics::try_new(builder.parquet_schema(), &bbox_paths)?;
            builder = apply_bbox_row_groups(builder, &bbox_cols, bbox)?;
            builder = apply_bbox_row_filter(builder, bbox_cols, bbox)?;
        }

        Ok(builder)
    }
}
