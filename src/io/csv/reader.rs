use geozero::csv::CsvReader;
use geozero::GeozeroDatasource;
use std::io::Read;

use crate::error::Result;
use crate::io::geozero::table::builder::GeoTableBuilder;
use crate::table::GeoTable;

pub fn read_csv<R: Read>(reader: R, geometry_column_name: &str) -> Result<GeoTable> {
    let mut csv = CsvReader::new(geometry_column_name, reader);
    let mut geo_table = GeoTableBuilder::<i32>::new();
    csv.process(&mut geo_table)?;
    geo_table.finish()
}
