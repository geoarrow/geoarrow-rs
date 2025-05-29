use arrow_schema::SchemaRef;
use geo_types::Rect;
use geoarrow_schema::CoordType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowReaderBuilder, RowFilter};

use crate::metadata::GeoParquetMetadata;
use crate::reader::parse::infer_geoarrow_schema;
use crate::reader::spatial_filter::{ParquetBboxStatistics, bbox_arrow_predicate, bbox_row_groups};

/// A trait that extends the [`ArrowReaderBuilder`] with methods for reading GeoParquet files.
///
/// Instead of creating _wrapper_ structs around the upstream
/// [`ParquetRecordBatchReaderBuilder`][parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder]
/// and
/// [`ParquetRecordBatchStreamBuilder`][parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder],
/// which would require the `geoparquet` crate to fully duplicate all of the builder functionality
/// of the upstream `parquet` crate, this trait provides a way to extend the upstream builders
/// directly with geospatial functionality.
///
/// Once you've finished modifying your builder, call `build` and then pass the resulting reader or
/// stream to a [`GeoParquetRecordBatchReader`][crate::reader::GeoParquetRecordBatchReader] or
/// [`GeoParquetRecordBatchStream`][crate::reader::GeoParquetRecordBatchStream], respectively, to
/// read the data.
pub trait GeoParquetReaderBuilder: Sized {
    /// Parse the geospatial metadata, if any, from the parquet file metadata.
    ///
    /// Returns `None` if the file does not contain geospatial metadata or if it is not valid.
    fn geoparquet_metadata(&self) -> Option<GeoParquetMetadata>;

    /// Convert the Arrow schema provided by the underlying [ArrowReaderBuilder] into one with
    /// GeoArrow metadata on each geometry column described in the GeoParquet metadata.
    ///
    /// The [`GeoParquetMetadata`] can be constructed from the
    /// [`geoparquet_metadata`][Self::geoparquet_metadata] method.
    ///
    /// If you wish to parse geometries to their native representation, set `parse_to_native` to
    /// `true`. If you want to leave geometries as WKB, set it to `false`.
    fn geoarrow_schema(
        &self,
        geo_metadata: &GeoParquetMetadata,
        parse_to_native: bool,
        coord_type: CoordType,
    ) -> GeoArrowResult<SchemaRef>;

    /// Construct a spatial [ArrowPredicate] that keeps rows that intersect with the provided
    /// `bbox`.
    ///
    /// Note that this will **replace** any existing [`RowFilter`]s. If you want to use more than
    /// one filter, you should create [`ArrowPredicate`s] directly and pass in your own
    /// [`RowFilter`] to [`ArrowReaderBuilder::with_row_filter`].
    ///
    /// Note that the `bbox` must be in the same coordinate system as the geometries in the
    /// designated geometry column.
    ///
    /// If `column_name` is `None`, the primary geometry column will be used.
    fn intersecting_arrow_predicate(
        &self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Box<dyn ArrowPredicate>>;

    /// Apply a spatial intersection [RowFilter] to this [`ArrowReaderBuilder`].
    ///
    /// Note that this will **replace** any existing [`RowFilter`]s on the builder. If you want to
    /// use more than one [`ArrowPredicate`] in your [`RowFilter`], use
    /// `Self::intersecting_arrow_predicate` to create the [`ArrowPredicate`] yourself. Then create
    /// your own [`RowFilter`] that you pass to [`ArrowReaderBuilder::with_row_filter`].
    ///
    /// If `column_name` is `None`, the primary geometry column will be used.
    fn with_intersecting_row_filter(
        self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Self>;

    /// Find the row groups that intersect with the bounding box.
    ///
    /// Note that the `bbox` must be in the same coordinate system as the geometries in the
    /// designated geometry column.
    ///
    /// If `column_name` is `None`, the primary geometry column will be used.
    fn intersecting_row_groups(
        &self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Vec<usize>>;

    /// Select row groups to read based on the bounding box.
    ///
    /// Note that this will **replace** any existing row group selection. If you want more detailed
    /// selection of row groups, use [`ArrowReaderBuilder::with_row_groups`] yourself.
    ///
    /// If `column_name` is `None`, the primary geometry column will be used.
    fn with_intersecting_row_groups(
        self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
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

    fn intersecting_arrow_predicate(
        &self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Box<dyn ArrowPredicate>> {
        let column_name = column_name.unwrap_or(&geo_metadata.primary_column);
        let covering =
            geo_metadata
                .bbox_covering(Some(column_name))?
                .ok_or(GeoArrowError::GeoParquet(format!(
                    "No covering metadata found for column: {:?}",
                    column_name
                )))?;

        let bbox_cols = ParquetBboxStatistics::try_new(self.parquet_schema(), &covering)?;

        bbox_arrow_predicate(self.parquet_schema(), bbox_cols, bbox)
    }

    fn with_intersecting_row_filter(
        self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Self> {
        let predicate = self.intersecting_arrow_predicate(bbox, geo_metadata, column_name)?;
        Ok(self.with_row_filter(RowFilter::new(vec![predicate])))
    }

    fn intersecting_row_groups(
        &self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Vec<usize>> {
        let column_name = column_name.unwrap_or(&geo_metadata.primary_column);
        let covering =
            geo_metadata
                .bbox_covering(Some(column_name))?
                .ok_or(GeoArrowError::GeoParquet(format!(
                    "No covering metadata found for column: {:?}",
                    column_name
                )))?;

        let bbox_cols = ParquetBboxStatistics::try_new(self.parquet_schema(), &covering)?;

        bbox_row_groups(self.metadata().row_groups(), &bbox_cols, bbox)
    }

    fn with_intersecting_row_groups(
        self,
        bbox: Rect,
        geo_metadata: &GeoParquetMetadata,
        column_name: Option<&str>,
    ) -> GeoArrowResult<Self> {
        let row_groups = self.intersecting_row_groups(bbox, geo_metadata, column_name)?;
        if row_groups.is_empty() {
            return Err(GeoArrowError::GeoParquet(
                "No row groups intersect with the bounding box".to_string(),
            ));
        }
        Ok(self.with_row_groups(row_groups))
    }
}
