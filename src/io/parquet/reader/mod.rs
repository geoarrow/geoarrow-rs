#[cfg(feature = "parquet_async")]
mod r#async;
mod builder;
mod metadata;
mod options;
mod parse;
mod spatial_filter;

pub use builder::{GeoParquetRecordBatchReader, GeoParquetRecordBatchReaderBuilder};
pub use metadata::{GeoParquetDatasetMetadata, GeoParquetReaderMetadata};
pub use options::GeoParquetReaderOptions;
#[cfg(feature = "parquet_async")]
pub use r#async::{GeoParquetRecordBatchStream, GeoParquetRecordBatchStreamBuilder};
pub use spatial_filter::ParquetBboxPaths;

#[allow(dead_code)]
pub(crate) fn parse_table_geometries_to_native(
    table: &mut crate::table::Table,
    metadata: &parquet::file::metadata::FileMetaData,
    coord_type: &crate::array::CoordType,
) -> crate::error::Result<()> {
    let geom_cols =
        super::metadata::find_geoparquet_geom_columns(metadata, table.schema(), *coord_type)?;
    geom_cols
        .iter()
        .try_for_each(|(geom_col_idx, target_geo_data_type)| {
            table.parse_geometry_to_native(*geom_col_idx, *target_geo_data_type)
        })
}
