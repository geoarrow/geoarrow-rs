use arrow_schema::SchemaRef;
use geo_types::Rect;
use geoarrow_schema::CoordType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;

use crate::metadata::{GeoParquetBboxCovering, GeoParquetMetadata};
use crate::reader::parse::infer_geoarrow_schema;
use crate::reader::spatial_filter::{
    ParquetBboxStatistics, apply_bbox_row_filter, apply_bbox_row_groups,
};

// TODO: allow passing in geo metadata so you don't have to parse it again each time in the other
// methods?

pub trait GeoParquetReaderBuilder: Sized {
    /// Parse the geospatial metadata, if any, from the parquet file metadata.
    ///
    /// Returns `None` if the file does not contain geospatial metadata or if it is not valid.
    fn geoparquet_metadata(&self) -> Option<GeoParquetMetadata>;

    /// Convert the Arrow schema provided by the underlying [ArrowReaderBuilder] into one with
    /// native GeoArrow geometries, based on the GeoParquet metadata.
    ///
    /// First construct the GeoParquet metadata from the
    /// [`geoparquet_metadata`][Self::geoparquet_metadata] method.
    fn geoarrow_schema(
        &self,
        geo_metadata: &GeoParquetMetadata,
        parse_to_native: bool,
        coord_type: CoordType,
    ) -> GeoArrowResult<SchemaRef>;

    /// Add a spatial [RowFilter] to this reader builder.
    ///
    /// Note that this will **replace** any existing [`RowFilter`]s. If you want to use more than
    /// one filter, you should create [`ArrowPredicate`s] directly and pass in your own
    /// [`RowFilter`] to [`ArrowReaderBuilder::with_row_filter`].
    fn with_spatial_row_filter(
        self,
        bbox: Rect,
        bbox_paths: Option<GeoParquetBboxCovering>,
    ) -> GeoArrowResult<Self>;

    /// Select row groups to read based on the bounding box.
    ///
    /// Note that this will **replace** any existing row group selection. If you want more detailed
    /// selection of row groups, use [`ArrowReaderBuilder::with_row_groups`] yourself.
    fn with_spatial_row_groups(
        self,
        bbox: Rect,
        bbox_paths: Option<GeoParquetBboxCovering>,
    ) -> GeoArrowResult<Self>;
}

impl<T> GeoParquetReaderBuilder for ArrowReaderBuilder<T> {
    fn geoparquet_metadata(&self) -> Option<GeoParquetMetadata> {
        GeoParquetMetadata::from_parquet_meta(self.metadata().file_metadata()).ok()
    }

    fn geoarrow_schema(
        &self,
        geo_metadata: &GeoParquetMetadata,
        parse_to_native: bool,
        coord_type: CoordType,
    ) -> GeoArrowResult<SchemaRef> {
        infer_geoarrow_schema(self.schema(), geo_metadata, parse_to_native, coord_type)
    }

    fn with_spatial_row_filter(
        self,
        bbox: Rect,
        bbox_paths: Option<GeoParquetBboxCovering>,
    ) -> GeoArrowResult<Self> {
        // TODO: deduplicate across these two args
        let bbox_paths = if let Some(paths) = bbox_paths {
            paths
        } else {
            let geo_meta = self.geoparquet_metadata().ok_or(GeoArrowError::GeoParquet(
                "No geospatial metadata and bbox paths were not passed".to_string(),
            ))?;

            geo_meta
                .bbox_covering(None)?
                .ok_or(GeoArrowError::GeoParquet(
                    "No covering metadata found".to_string(),
                ))?
        };

        let bbox_cols = ParquetBboxStatistics::try_new(self.parquet_schema(), &bbox_paths)?;

        apply_bbox_row_filter(self, bbox_cols, bbox)
    }

    fn with_spatial_row_groups(
        self,
        bbox: Rect,
        bbox_paths: Option<GeoParquetBboxCovering>,
    ) -> GeoArrowResult<Self> {
        let bbox_paths = if let Some(paths) = bbox_paths {
            paths
        } else {
            let geo_meta = self.geoparquet_metadata().ok_or(GeoArrowError::GeoParquet(
                "No geospatial metadata and bbox paths were not passed".to_string(),
            ))?;

            geo_meta
                .bbox_covering(None)?
                .ok_or(GeoArrowError::GeoParquet(
                    "No covering metadata found".to_string(),
                ))?
        };

        let bbox_cols = ParquetBboxStatistics::try_new(self.parquet_schema(), &bbox_paths)?;

        apply_bbox_row_groups(self, &bbox_cols, bbox)
    }
}
