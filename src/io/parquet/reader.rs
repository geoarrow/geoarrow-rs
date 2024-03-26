use crate::array::CoordType;
use crate::error::Result;
use crate::io::parquet::metadata::build_arrow_schema;
use crate::table::GeoTable;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;

pub struct GeoParquetReaderOptions {
    /// The number of rows in each batch.
    pub batch_size: usize,

    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    pub bbox: Option<(f64, f64, f64, f64)>,
}

impl Default for GeoParquetReaderOptions {
    fn default() -> Self {
        Self {
            batch_size: 65535,
            coord_type: Default::default(),
            bbox: None,
        }
    }
}

/// Read a GeoParquet file to a GeoTable.
pub fn read_geoparquet<R: ChunkReader + 'static>(
    reader: R,
    options: GeoParquetReaderOptions,
) -> Result<GeoTable> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(reader)?.with_batch_size(options.batch_size);

    let (arrow_schema, geometry_column_index, target_geo_data_type) =
        build_arrow_schema(&builder, &options.coord_type)?;

    let reader = builder.build()?;

    let mut batches = vec![];
    for maybe_batch in reader {
        batches.push(maybe_batch?);
    }

    GeoTable::from_arrow(
        batches,
        arrow_schema,
        Some(geometry_column_index),
        target_geo_data_type,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;

    #[test]
    fn nybb() {
        let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
        let options = Default::default();
        let _output_ipc = read_geoparquet(file, options).unwrap();
    }
}
