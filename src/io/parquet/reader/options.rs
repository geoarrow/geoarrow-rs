use geo::Rect;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::parquet::reader::spatial_filter::{
    apply_bbox_row_groups, ParquetBboxPaths, ParquetBboxStatistics,
};

/// Options for reading (Geo)Parquet
///
/// Geospatial options will only be applied if the target file has geospatial metadata.
#[derive(Clone, Default)]
pub struct ParquetReaderOptions {
    /// The number of rows in each batch. If not provided, the upstream [parquet] default is 1024.
    pub batch_size: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_limit]
    pub limit: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_offset]
    pub offset: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_projection]
    pub projection: Option<ProjectionMask>,

    /// The GeoArrow coordinate type to use in the geometry arrays.
    ///
    /// Note that for now this is only used when parsing from WKB-encoded geometries.
    pub coord_type: CoordType,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    pub bbox: Option<Rect>,

    /// The paths in the Parquet schema to the bounding box columns. This will not be necessary as
    /// of GeoParquet 1.1.
    pub bbox_paths: Option<ParquetBboxPaths>,
}

impl ParquetReaderOptions {
    pub fn apply_to_builder<T>(
        self,
        mut builder: ArrowReaderBuilder<T>,
    ) -> Result<ArrowReaderBuilder<T>> {
        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }

        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }

        if let Some(projection) = self.projection {
            builder = builder.with_projection(projection);
        }

        if let (Some(bbox), Some(bbox_paths)) = (self.bbox, self.bbox_paths) {
            let bbox_cols = ParquetBboxStatistics::try_new(builder.parquet_schema(), &bbox_paths)?;
            builder = apply_bbox_row_groups(builder, bbox_cols, bbox)?;
            // Need to fix the column ordering of the row filter inside construct_predicate
            // builder = apply_bbox_row_filter(builder, bbox_cols, bbox)?;
        }

        Ok(builder)
    }
}
