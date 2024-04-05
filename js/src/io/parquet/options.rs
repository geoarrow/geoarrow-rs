use geoarrow::array::CoordType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct JsParquetReaderOptions {
    /// The number of rows in each batch. If not provided, the upstream [parquet] default is 1024.
    pub batch_size: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_limit]
    pub limit: Option<usize>,

    /// See [parquet::arrow::arrow_reader::ArrowReaderBuilder::with_offset]
    pub offset: Option<usize>,
}

impl From<JsParquetReaderOptions> for geoarrow::io::parquet::ParquetReaderOptions {
    fn from(value: JsParquetReaderOptions) -> Self {
        Self {
            batch_size: value.batch_size,
            limit: value.limit,
            offset: value.offset,
            projection: None,
            coord_type: CoordType::Interleaved,
            bbox: None,
            bbox_paths: None,
        }
    }
}
