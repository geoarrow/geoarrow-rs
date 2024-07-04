use std::sync::Arc;

use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use serde_json::Value;

use crate::array::{CoordType, PolygonArray, RectBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::GeoParquetMetadata;
use crate::io::parquet::reader::parse::infer_target_schema;
use crate::io::parquet::reader::spatial_filter::ParquetBboxStatistics;
use crate::io::parquet::ParquetBboxPaths;

#[derive(Debug, Clone)]
pub struct GeoParquetReaderMetadata {
    meta: ArrowReaderMetadata,
    geo_meta: Option<GeoParquetMetadata>,
}

impl GeoParquetReaderMetadata {
    pub fn new(meta: ArrowReaderMetadata) -> Self {
        let geo_meta = GeoParquetMetadata::from_parquet_meta(meta.metadata().file_metadata()).ok();
        Self { meta, geo_meta }
    }

    pub fn arrow_metadata(&self) -> &ArrowReaderMetadata {
        &self.meta
    }

    /// Access the geo metadata of this file.
    pub fn geo_metadata(&self) -> Option<&GeoParquetMetadata> {
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

    pub fn resolved_schema(&self, coord_type: CoordType) -> Result<SchemaRef> {
        if let Some(geo_meta) = &self.geo_meta {
            infer_target_schema(self.meta.schema(), geo_meta, coord_type)
        } else {
            // If non-geospatial, return the same schema as output
            Ok(self.meta.schema().clone())
        }
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

    /// Get the bounds of a single row group.
    ///
    /// As of GeoParquet 1.1 you won't need to pass in these column names, as they'll be specified
    /// in the metadata.
    pub fn row_group_bounds(
        &self,
        row_group_idx: usize,
        paths: &ParquetBboxPaths,
    ) -> Result<Option<geo::Rect>> {
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
}
