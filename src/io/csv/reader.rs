use geozero::csv::CsvReader;
use geozero::GeozeroDatasource;
use std::io::Read;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::builder::GeoTableBuilder;
use crate::io::geozero::table::GeoTableBuilderOptions;
use crate::table::GeoTable;

pub struct CSVReaderOptions {
    pub coord_type: CoordType,
    pub batch_size: usize,
}

impl CSVReaderOptions {
    pub fn new(coord_type: CoordType, batch_size: usize) -> Self {
        Self {
            coord_type,
            batch_size,
        }
    }
}

impl Default for CSVReaderOptions {
    fn default() -> Self {
        Self::new(Default::default(), 65_536)
    }
}

pub fn read_csv<R: Read>(
    reader: R,
    geometry_column_name: &str,
    options: CSVReaderOptions,
) -> Result<GeoTable> {
    let mut csv = CsvReader::new(geometry_column_name, reader);
    let table_builder_options = GeoTableBuilderOptions::new(
        options.coord_type,
        true,
        Some(options.batch_size),
        None,
        None,
    );
    let mut geo_table =
        GeoTableBuilder::<MixedGeometryStreamBuilder<i32>>::new_with_options(table_builder_options);
    csv.process(&mut geo_table)?;
    geo_table.finish()
}
