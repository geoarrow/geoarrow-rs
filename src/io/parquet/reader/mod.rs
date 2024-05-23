#[cfg(feature = "parquet_async")]
mod r#async;
mod options;
// mod parse;
mod spatial_filter;
mod sync;

pub use options::ParquetReaderOptions;
#[cfg(feature = "parquet_async")]
pub use r#async::{read_geoparquet_async, ParquetDataset, ParquetFile};
pub use spatial_filter::ParquetBboxPaths;
pub use sync::read_geoparquet;

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
